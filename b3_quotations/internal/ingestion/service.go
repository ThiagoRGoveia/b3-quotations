package ingestion

import (
	"log"
	"sync"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/config"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/database"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
)

type IngestionService struct {
	dbManager     database.DBManager
	asyncWorker   Worker
	fileProcessor Processor
	config        config.Config
}

func NewIngestionService(dbManager database.DBManager, worker Worker, processor Processor, cfg config.Config) *IngestionService {
	return &IngestionService{
		dbManager:     dbManager,
		asyncWorker:   worker,
		fileProcessor: processor,
		config:        cfg,
	}
}

// Execute orchestrates the file processing workflow.
func (h *IngestionService) Execute(filesPath string) error {
	// Step 0: Setup the extraction environment.
	setupReturn, err := h.setup(filesPath)

	if err != nil {
		return err
	}

	// Drop indexes before starting for efficiency
	h.dbManager.DropTradeRecordIndexes()
	// Make sure indexes are created after the process
	defer h.dbManager.CreateTradeRecordIndexes()

	fileInfo, channels, waitGroups, fileMap, fileErrorsMap, createdPartitions, cleanup := setupReturn.GetValues()
	defer cleanup()

	// Step 1: Preprocess files and send jobs to the parser workers.
	// This is done in a goroutine to allow the main flow to continue with worker setup.
	go h.fileProcessor.PreprocessAndDispatchJobs(fileInfo, *fileMap, channels.Jobs)

	h.asyncWorker.WithChannels(channels).WithWaitGroups(waitGroups)

	// Step 2: Setup the error worker, this worker will handle async errors from the extraction process
	errorWorkerRunner, errorWorkerWaitGroup, err := h.asyncWorker.SetupErrorWorker()
	if err != nil {
		return err
	}
	// Step 2.1: Start error worker
	errorWorkerRunner.Run(fileErrorsMap)

	// Step 3: Setup parser workers, these workers will handle the file parsing to read jobs from
	// jobs channel
	parserWorkersRunner, parserWorkerWaitGroup, err := h.asyncWorker.SetupParserWorkers(h.config.NumParserWorkers)
	if err != nil {
		return err
	}

	// Step 3.1: Start parser workers
	parserWorkersRunner.Run()

	// Step 4: Configure DB workers. This function will return a factory function to start the DB workers goroutines.
	dbWorkersRunner, dbWorkerWaitGroup, err := h.asyncWorker.SetupDBWorkers(h.config.NumDBWorkersPerReferenceDate)

	if err != nil {
		return err
	}

	// Step 5: Start DB workers. And pass handler that will handle cases where the partition is empty or not.
	err = dbWorkersRunner.Run(func(trades *[]*models.Trade, stagingTableName string) error {
		if isPartitionFirstWrite((*trades)[0].ReferenceDate, createdPartitions) {
			//empty partition can benefit for insert without idempotency checks
			return h.dbManager.InsertAllStagingTableData(*trades, stagingTableName)
		} else {
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

	// Step 6.3: Wait for file error worker to finish
	log.Println("Waiting for file error worker to finish...")
	errorWorkerWaitGroup.Wait()

	// Step 6.4: Close the errors channel after all workers that can produce errors are done.
	close(channels.Errors)

	// Step 7: Update file status
	h.fileProcessor.UpdateFileStatus(fileErrorsMap, fileMap)

	log.Println("Extraction process finished.")
	return nil
}

func (h *IngestionService) setup(filesPath string) (models.SetupReturn, error) {
	// Step 0.1: Setup channels, waitgroups, file map, and error map.
	jobs := make(chan models.FileProcessingJob, 100)
	errors := make(chan models.AppError, 100)

	channels := models.ExtractionChannels{
		Results: make(map[time.Time]chan *models.Trade),
		Errors:  errors,
		Jobs:    jobs,
	}

	var parserWg, dbWg, errorWg sync.WaitGroup
	fileMap := make(map[int]string)
	fileErrorsMap := models.FileErrorMap{Errors: make(map[int][]models.AppError)}
	// Step 0.2: Synchronously get all file paths and their reference dates.
	log.Println("Scanning files to determine required partitions...")
	referenceDates, err := h.fileProcessor.ScanForFiles(filesPath)
	if err != nil {
		log.Fatalf("Failed to scan files: %v", err)
		return models.SetupReturn{}, err
	}

	// Step 0.3: Setup the database and get the cleanup function.
	cleanup, createdPartitions, err := h.setupDatabase(referenceDates, &channels)
	if err != nil {
		log.Fatalf("Failed to setup database: %v", err)
		return models.SetupReturn{}, err
	}

	return models.SetupReturn{
		FileInfo:          referenceDates,
		Channels:          &channels,
		WaitGroups:        &models.ExtractionWaitGroups{ParserWg: &parserWg, DbWg: &dbWg, ErrorWg: &errorWg},
		FileMap:           &fileMap,
		FileErrorsMap:     &fileErrorsMap,
		CreatedPartitions: createdPartitions,
		Cleanup:           cleanup,
	}, nil
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

	// Create all needed partitions in a single transaction
	createdPartitions, err := h.dbManager.CreatePartitionsForDates(dates)
	if err != nil {
		log.Fatalf("Failed to create partitions: %v", err)
		return nil, createdPartitions, err
	}

	// Calculate the total number of DB workers (numDates * NumDBWorkersPerReferenceDate)
	totalDBWorkers := len(uniqueDates) * h.config.NumDBWorkersPerReferenceDate
	log.Printf("Creating %d staging tables for %d dates with %d workers per date", totalDBWorkers, len(uniqueDates), h.config.NumDBWorkersPerReferenceDate)

	// Create staging tables for each DB worker in a single transaction
	stagingTableNames, err := h.dbManager.CreateWorkerStagingTables(totalDBWorkers)
	if err != nil {
		log.Fatalf("Failed to create staging tables: %v", err)
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
