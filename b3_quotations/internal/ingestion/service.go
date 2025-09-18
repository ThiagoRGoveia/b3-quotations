package ingestion

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/config"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/database"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
)

type IngestionService struct {
	dbManager   database.DBManager
	asyncWorker AsyncWorker
	config      config.Config
}

func NewIngestionService(dbManager database.DBManager, numParserWorkers int, numDBWorkersPerDate int, dbBatchSize int, resultsChannelSize int) *IngestionService {
	return &IngestionService{
		dbManager: dbManager,
		asyncWorker: *NewAsyncWorker(dbManager, AsyncWorkerConfig{
			NumParserWorkers:             numParserWorkers,
			NumDBWorkersPerReferenceDate: numDBWorkersPerDate,
			DBBatchSize:                  dbBatchSize,
		}),
		config: config.Config{
			NumParserWorkers:             numParserWorkers,
			NumDBWorkersPerReferenceDate: numDBWorkersPerDate,
			DBBatchSize:                  dbBatchSize,
			ResultsChannelSize:           resultsChannelSize,
		},
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

	// Step 1: Start the error worker, this worker will handle async errors from the extraction process
	waitGroups.ErrorWg.Add(1)
	go h.asyncWorker.ErrorWorker(channels, fileErrorsMap, waitGroups)

	// Step 2: Start parser workers, these workers will handle the file parsing
	for w := 1; w <= h.config.NumParserWorkers; w++ {
		waitGroups.ParserWg.Add(1)
		go h.asyncWorker.ParserWorker(channels, waitGroups)
	}

	// Step 3: Configure DB workers. This function will return a factory function to start the DB workers goroutines.
	dbWorkers := h.configDbWorkers(channels, waitGroups)

	// Step 4: Start DB workers. And pass handler that will handle cases where the partition is empty or not.
	err = dbWorkers(func(trades *[]*models.Trade, stagingTableName string) error {
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

	// Step 5: Preprocess files and send jobs to the parser workers.
	// This needs to be in a goroutine so that the main thread can close the jobs channel.
	go func() {
		preprocessAndDispatchJobs(fileInfo, h.dbManager, *fileMap, channels.Jobs)
		close(channels.Jobs) // Close the jobs channel after all jobs have been sent.
	}()

	// Step 6: Wait for all processing to complete.
	log.Println("Waiting for parser workers to finish...")
	waitGroups.ParserWg.Wait()

	// After parsers are done, close all date-specific results channels to signal DB workers to finish.
	for _, resultsChan := range channels.Results {
		close(resultsChan)
	}

	log.Println("Waiting for DB workers to finish...")
	waitGroups.DbWg.Wait()

	// Close the errors channel after all workers that can produce errors are done.
	close(channels.Errors)

	// TODO: This needs cleanup, it does not need to be a separate worker, can be sync, also
	// needs error handling, and the updates can be batched.
	waitGroups.ErrorWg.Add(1)
	go h.asyncWorker.FileStatusWorker(fileErrorsMap, waitGroups, fileMap)

	// Wait for the error and status workers to finish
	waitGroups.ErrorWg.Wait()

	log.Println("Extraction process finished.")
	return nil
}

func (h *IngestionService) setup(filesPath string) (models.SetupReturn, error) {
	// Step 0.1: Setup channels, waitgroups, file map, and error map.
	jobs := make(chan models.FileProcessingJob, 100)
	errors := make(chan models.AppError, 100)

	// Initialize the channels struct with empty map
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
	referenceDates, err := getReferenceDates(filesPath)
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

	log.Println("Database setup complete. All necessary tables and partitions are ready.")

	// Step 0.5: Start all worker pools.
	log.Println("Starting worker pools...")
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

func (h *IngestionService) configDbWorkers(channels *models.ExtractionChannels, waitGroups *models.ExtractionWaitGroups) func(func(*[]*models.Trade, string) error) error {
	return func(dbHandler func(*[]*models.Trade, string) error) error {
		workerCounter := 1
		for date, resultsChan := range channels.Results {
			log.Printf("Starting %d DB workers for date %s", h.config.NumDBWorkersPerReferenceDate, date.Format("2006-01-02"))
			for w := 1; w <= h.config.NumDBWorkersPerReferenceDate; w++ {
				workerId := workerCounter
				stagingTableName := fmt.Sprintf("trade_records_staging_worker_%d", workerId)
				waitGroups.DbWg.Add(1)
				go h.asyncWorker.DbWorker(workerId, stagingTableName, resultsChan, channels.Errors, waitGroups, dbHandler)
				workerCounter++
			}
		}
		return nil
	}
}

// FileStatusWorker updates the final status of each processed file based on whether errors occurred.
func (h *IngestionService) setupDatabase(fileInfoList []models.FileInfo, channels *models.ExtractionChannels) (func(), *models.FirstWritePartition, error) {
	// Create database tables // TEMP move this into docker setup
	h.dbManager.CreateFileRecordsTable()
	h.dbManager.CreateTradeRecordsTable()
	h.dbManager.CreateTradeRecordIndexes()

	// Build a set of unique dates from the file information.
	uniqueDates := make(map[time.Time]struct{})
	for _, fileInfo := range fileInfoList {
		normalizedDate := fileInfo.ReferenceDate.Truncate(24 * time.Hour)
		uniqueDates[normalizedDate] = struct{}{}

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

	// Calculate the total number of DB workers (numDates * numDBWorkersPerDate)
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

func isPartitionFirstWrite(partitionName time.Time, partitionMap *models.FirstWritePartition) bool {
	wasCreatedThisRun := (*partitionMap)[partitionName]
	return wasCreatedThisRun
}
