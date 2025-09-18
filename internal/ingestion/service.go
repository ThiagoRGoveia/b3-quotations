package ingestion

import (
	"log"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/config"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/database"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
)

type IngestionService struct {
	dbManager     database.DBManager
	setupService  ISetup
	asyncWorker   Worker
	fileProcessor Processor
	config        config.Config
}

func NewIngestionService(dbManager database.DBManager, setupService ISetup, worker Worker, processor Processor, cfg config.Config) *IngestionService {
	return &IngestionService{
		dbManager:     dbManager,
		setupService:  setupService,
		asyncWorker:   worker,
		fileProcessor: processor,
		config:        cfg,
	}
}

// Execute orchestrates the file processing workflow.
func (h *IngestionService) Execute(filesPath string) error {
	// Step 0: Setup the extraction environment.
	environmentConfig, err := h.setupService.build()

	if err != nil {
		return err
	}

	channels, waitGroups, fileMap, fileErrorsMap := environmentConfig.GetValues()
	// Step 0.1: Setup the extraction environment.
	log.Println("Scanning files to determine required partitions...")
	referenceDates, err := h.fileProcessor.ScanForFiles(filesPath)
	if err != nil {
		log.Printf("Failed to scan files: %v", err)
		return err
	}

	// Step 0.2: Setup the database and get the cleanup function.
	cleanup, createdPartitions, err := h.setupDatabase(referenceDates, channels)
	if err != nil {
		log.Printf("Failed to setup database: %v", err)
		return err
	}
	defer cleanup()
	defer func() {
		log.Println("Re-creating trade record indexes...")
		h.dbManager.CreateTradeRecordIndexes()
	}()

	// Step 0.3: Drop indexes before starting for efficiency
	log.Println("Dropping trade record indexes...")
	h.dbManager.DropTradeRecordIndexes()
	// Step 0.4: Make sure indexes are created after the process

	// Step 0.5: Setup the async worker channels and wait groups VERY IMPORTANT: can cause panic if not done
	h.asyncWorker.WithChannels(channels).WithWaitGroups(waitGroups)

	// Step 1: Preprocess files and send jobs to the parser workers.
	// - Gets reference dates from files
	// - Calculates check sum for files and checks if they are already processed
	// - Saves file record to db
	// This is done in a goroutine to allow the main flow to continue with worker setup.
	// Sharing MainWg with error worker
	dispatcherWorkerRunner, _, err := h.asyncWorker.SetupJobDispatcherWorker(referenceDates, *fileMap)
	if err != nil {
		return err
	}
	dispatcherWorkerRunner.Run()

	// Step 2: Setup the error worker, this worker will handle async errors from the extraction process
	// Sharing MainWg with dispatcher worker
	errorWorkerRunner, mainWaitGroup, err := h.asyncWorker.SetupErrorWorker()
	if err != nil {
		return err
	}
	// Step 2.1: Start error worker
	errorWorkerRunner.Run(fileErrorsMap)

	// Step 3: Setup parser workers
	// - Extract data from CSV
	// - Send trade data to channel, there is one channel per date
	parserWorkersRunner, parserWorkerWaitGroup, err := h.asyncWorker.SetupParserWorkers(h.config.NumParserWorkers)
	if err != nil {
		return err
	}

	// Step 3.1: Start parser workers
	parserWorkersRunner.Run()

	// Step 4: Configure DB workers. This function will return a factory function to start the DB workers goroutines.
	// - DB workers will handle the idempotency checks
	// - DB workers will handle the partition checks
	// - Each trade channel has e configurable number of db workers reading from it
	dbWorkersRunner, dbWorkerWaitGroup, err := h.asyncWorker.SetupDBWorkers(h.config.NumDBWorkersPerReferenceDate)

	if err != nil {
		return err
	}

	// Step 5: Start DB workers. Here we pass a handler treating the case for idempotency for better readability
	err = dbWorkersRunner.Run(func(trades *[]*models.Trade, stagingTableName string) error {
		if isPartitionFirstWrite((*trades)[0].ReferenceDate, createdPartitions) {
			//empty partition can benefit from insert without idempotency checks which are faster
			return h.dbManager.InsertAllStagingTableData(*trades, stagingTableName)
		} else {
			// partitions already exists, means there might be data on it, gotta check for duplicates
			return h.dbManager.InsertDiffFromStagingTable(*trades, stagingTableName)
		}
	})

	if err != nil {
		return err
	}

	// Step 6: Wait for all processing to complete.
	log.Println("Waiting for parser workers to finish...")
	parserWorkerWaitGroup.Wait()

	// Step 6.1: After parsers are done, close all date-specific results channels to signal DB workers to finish.
	for _, resultsChan := range channels.Results {
		close(resultsChan)
	}

	// Step 6.2: Wait for DB workers to finish
	log.Println("Waiting for DB workers to finish...")
	dbWorkerWaitGroup.Wait()

	// Step 6.3: Close the errors channel after all workers that can produce errors are done.
	close(channels.Errors)

	// Step 6.4: Wait for file error worker to finish
	log.Println("Waiting for file error worker to finish...")
	mainWaitGroup.Wait()

	// Step 7: Update each file record with status of operation and errors with results from o
	h.fileProcessor.UpdateFileStatus(fileErrorsMap, fileMap)

	log.Println("Extraction process finished.")
	return nil
}

func (h *IngestionService) setupDatabase(fileInfoList []models.FileInfo, channels *models.ExtractionChannels) (func(), *models.FirstWritePartition, error) {
	// Build a set of unique dates from the file information. This will guide the partition management.
	uniqueDates := make(map[time.Time]bool)
	for _, fileInfo := range fileInfoList {
		normalizedDate := fileInfo.ReferenceDate.Truncate(24 * time.Hour)
		uniqueDates[normalizedDate] = true

		// Create a results channel for this date if it doesn't exist yet
		if _, exists := channels.Results[normalizedDate]; !exists {
			channels.Results[normalizedDate] = make(chan *models.Trade, h.config.ResultsChannelSize)
			log.Printf("Created results channel for date: %s", normalizedDate.Format("2006-01-02"))
		}
	}
	log.Printf("Found %d unique dates. Ensuring partitions exist...", len(uniqueDates))

	// Convert the unique dates map to a slice for the batch operation
	dates := make([]time.Time, 0, len(uniqueDates))
	for date := range uniqueDates {
		dates = append(dates, date)
	}

	// Create all needed partitions based in the unique dates processed
	createdPartitions, err := h.dbManager.CreatePartitionsForDates(dates)
	if err != nil {
		log.Printf("Failed to create partitions: %v", err)
		return nil, createdPartitions, err
	}

	// Calculate the total number of DB workers (numDates * NumDBWorkersPerReferenceDate)
	totalDBWorkers := len(uniqueDates) * h.config.NumDBWorkersPerReferenceDate
	log.Printf("Creating %d staging tables for %d dates with %d workers per date", totalDBWorkers, len(uniqueDates), h.config.NumDBWorkersPerReferenceDate)

	// Create staging tables for each DB worker to work in isolation
	stagingTableNames, err := h.dbManager.CreateWorkerStagingTables(totalDBWorkers)
	if err != nil {
		log.Printf("Failed to create staging tables: %v", err)
		return nil, createdPartitions, err
	}

	// Return a cleanup function to be deferred by the caller.
	return func() {
		for _, tableName := range stagingTableNames {
			log.Printf("Cleaning up staging table %s", tableName)
			h.dbManager.DropWorkerStagingTable(tableName)
		}
	}, createdPartitions, nil
}
