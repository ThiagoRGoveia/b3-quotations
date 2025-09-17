package handlers

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/db"
	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/models"
	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/parsers"
)

type ExtractionHandler struct {
	dbManager db.DBManager
	config    Config
}

func NewExtractionHandler(dbManager db.DBManager, numParserWorkers int, numDBWorkersPerDate int, dbBatchSize int, resultsChannelSize int) *ExtractionHandler {
	return &ExtractionHandler{
		dbManager: dbManager,
		config: Config{
			numParserWorkers:             numParserWorkers,
			numDBWorkersPerReferenceDate: numDBWorkersPerDate,
			dbBatchSize:                  dbBatchSize,
			resultsChannelSize:           resultsChannelSize,
		},
	}
}

// Execute orchestrates the file processing workflow.
func (h *ExtractionHandler) Execute(filesPath string) error {
	// Step 0: Setup the extraction environment.
	channels, waitGroups, fileMap, fileErrorsMap, createdPartitions, err := h.setup(filesPath)
	if err != nil {
		return err
	}

	// Step 1: Start the error worker, this worker will handle async errors from the extraction process
	waitGroups.errorWg.Add(1)
	go h.errorWorker(channels, fileErrorsMap, waitGroups)

	// Step 2: Start parser workers, these workers will handle the file parsing
	for w := 1; w <= h.config.numParserWorkers; w++ {
		waitGroups.parserWg.Add(1)
		go h.parserWorker(channels, waitGroups)
	}

	// Step 3: Configure DB workers. This function will return a factory function to start the DB workers goroutines.
	dbWorkers := h.configDbWorkers(channels, waitGroups)

	// Step 4: Start DB workers. And pass handler that will handle cases where the partition is empty or not.
	err = dbWorkers(func(trades *[]*models.Trade, stagingTableName string) error {
		if isPartitionEmpty((*trades)[0].ReferenceDate, createdPartitions) {
			//empty partition can benefit for insert without idempotency checks
			return h.dbManager.InsertAllStagingTableData(*trades, stagingTableName)
		} else {
			return h.dbManager.InsertDiffFromStagingTable(*trades, stagingTableName)
		}
	})
	if err != nil {
		return err
	}

	// Step 2: Wait for all processing to complete.
	log.Println("Waiting for all workers to finish...")
	waitGroups.parserWg.Wait()

	// Close all date-specific result channels
	for _, resultsChan := range channels.results {
		close(resultsChan)
	}

	close(channels.jobs)
	waitGroups.dbWg.Wait()
	close(channels.errors)

	// TODO: This needs cleanup, it does not need to be a separate worker, can be sync, also
	// needs error handling, and the updates can be batched.
	waitGroups.errorWg.Add(1)
	go h.fileStatusWorker(fileErrorsMap, waitGroups, fileMap)

	// Wait for the error and status workers to finish
	waitGroups.errorWg.Wait()

	log.Println("Extraction process finished.")
	return nil
}

func (h *ExtractionHandler) setup(filesPath string) (*ExtractionChannels, *ExtractionWaitGroups, *FileMap, *FileErrorMap, *db.FirstWritePartition, error) {
	// Step 0.1: Setup channels, waitgroups, file map, and error map.
	jobs := make(chan models.FileProcessingJob, 100)
	errors := make(chan models.AppError, 100)

	// Initialize the channels struct with empty map
	channels := ExtractionChannels{
		results: make(map[time.Time]chan *models.Trade),
		errors:  errors,
		jobs:    jobs,
	}

	var parserWg, dbWg, errorWg sync.WaitGroup
	fileMap := make(map[int]string)
	fileErrorsMap := FileErrorMap{errors: make(map[int][]models.AppError)}
	// Step 0.2: Synchronously get all file paths and their reference dates.
	log.Println("Scanning files to determine required partitions...")
	fileInfo, err := h.buildDatesSetAndFiles(filesPath)
	if err != nil {
		log.Fatalf("Failed to scan files: %v", err)
		return nil, nil, nil, nil, nil, err
	}

	// Step 0.3: Setup the database and get the cleanup function.
	cleanup, createdPartitions, err := h.setupDatabase(fileInfo, &channels)
	if err != nil {
		log.Fatalf("Failed to setup database: %v", err)
		return nil, nil, nil, nil, nil, err
	}
	defer cleanup()
	log.Println("Database setup complete. All necessary tables and partitions are ready.")

	// Step 0.4: Preprocess files.
	log.Println("Preprocessing files...")
	h.preprocessFile(fileInfo, &fileMap, &channels)

	// Step 0.5: Start all worker pools.
	log.Println("Starting worker pools...")
	return &channels, &ExtractionWaitGroups{parserWg: &parserWg, dbWg: &dbWg, errorWg: &errorWg}, &fileMap, &fileErrorsMap, createdPartitions, nil
}

func (h *ExtractionHandler) configDbWorkers(channels *ExtractionChannels, waitGroups *ExtractionWaitGroups) func(func(*[]*models.Trade, string) error) error {
	return func(dbHandler func(*[]*models.Trade, string) error) error {
		workerCounter := 1
		for date, resultsChan := range channels.results {
			log.Printf("Starting %d DB workers for date %s", h.config.numDBWorkersPerReferenceDate, date.Format("2006-01-02"))
			for w := 1; w <= h.config.numDBWorkersPerReferenceDate; w++ {
				workerId := workerCounter
				stagingTableName := fmt.Sprintf("trade_records_staging_worker_%d", workerId)
				waitGroups.dbWg.Add(1)
				go h.dbWorker(workerId, stagingTableName, resultsChan, channels.errors, waitGroups, dbHandler)
				workerCounter++
			}
		}
		return nil
	}
}

func (h *ExtractionHandler) preprocessFile(fileInfo []models.FileInfo, fileMap *FileMap, channels *ExtractionChannels) {
	for _, fileInfo := range fileInfo {
		// Calculate file checksum
		checksum, err := parsers.GetFileChecksum(fileInfo.Path)
		if err != nil {
			log.Printf("Error calculating checksum for file %s: %v\n", fileInfo.Path, err)
			continue // Skip file if checksum fails
		}

		// Synchronously create file record and get ID
		fileID, err := h.dbManager.InsertFileRecord(fileInfo.Path, time.Now(), db.FILE_STATUS_PROCESSING, checksum, fileInfo.ReferenceDate)
		if err != nil {
			log.Printf("Error inserting file record for %s: %v\n", fileInfo.Path, err)
			continue // Continue to next file
		}
		(*fileMap)[fileID] = fileInfo.Path
		channels.jobs <- models.FileProcessingJob{FilePath: fileInfo.Path, FileID: fileID}
	}
}

// parserWorker reads file jobs from a channel, parses the files, and sends the results to appropriate date-specific channels.
func (h *ExtractionHandler) parserWorker(channels *ExtractionChannels, waitGroups *ExtractionWaitGroups) {
	defer waitGroups.parserWg.Done()
	for job := range channels.jobs {
		log.Printf("Parser worker started job for file %s (ID: %d)\n", job.FilePath, job.FileID)
		err := parsers.ParseCSV(job.FilePath, job.FileID, channels.results, channels.errors)
		if err != nil {
			channels.errors <- models.AppError{FileID: job.FileID, Message: "Failed to open or read file", Err: err}
		}
		log.Printf("Parser worker finished job for file %s (ID: %d)\n", job.FilePath, job.FileID)
	}
}

// dbWorker processes trades from a specific date channel and inserts them into the database in batches.
func (h *ExtractionHandler) dbWorker(workerId int, stagingTableName string, resultsChan <-chan *models.Trade, errorsChan chan<- models.AppError, waitGroups *ExtractionWaitGroups, dbHandler func(*[]*models.Trade, string) error) {
	defer waitGroups.dbWg.Done()
	trades := make([]*models.Trade, 0, h.config.dbBatchSize)

	for result := range resultsChan {
		trades = append(trades, result)
		if len(trades) >= h.config.dbBatchSize {
			log.Printf("DB Worker %d: Inserting batch of %d trades using table %s\n", workerId, len(trades), stagingTableName)
			err := dbHandler(&trades, stagingTableName)
			if err != nil {
				// The batch failed, so report an error for each unique FileID in the batch.
				fileIDs := make(map[int]bool)
				for _, trade := range trades {
					fileIDs[trade.FileID] = true
				}
				for fileID := range fileIDs {
					errorsChan <- models.AppError{FileID: fileID, Message: "Failed to insert batch of trades", Err: err}
				}
			}
			trades = trades[:0] // Clear the slice
		}
	}

	// Insert any remaining trades
	if len(trades) > 0 {
		log.Printf("DB Worker %d: Inserting final batch of %d trades using table %s\n", workerId, len(trades), stagingTableName)
		// Using the DB manager's stored context
		err := h.dbManager.InsertMultipleTrades(trades, stagingTableName)
		if err != nil {
			// The batch failed, so report an error for each unique FileID in the batch.
			fileIDs := make(map[int]bool)
			for _, trade := range trades {
				fileIDs[trade.FileID] = true
			}
			for fileID := range fileIDs {
				errorsChan <- models.AppError{FileID: fileID, Message: "Failed to insert remaining batch of trades", Err: err}
			}
		}
	}

	log.Printf("DB worker %d finished.", workerId)
}

// errorWorker listens on the error channel, logs the errors, and tracks them by FileID.
func (h *ExtractionHandler) errorWorker(channels *ExtractionChannels, fileErrorsMap *FileErrorMap, waitGroups *ExtractionWaitGroups) {
	defer waitGroups.errorWg.Done()
	for appErr := range channels.errors {
		log.Printf("Caught error: %s\n", appErr.Error())
		// limit the number of errors per file to prevent memory overflow, if more than 100 errors are collected, then file is probably malformed
		if appErr.FileID != -1 && len(fileErrorsMap.errors) < 100 {
			fileErrorsMap.mu.Lock()
			fileErrorsMap.errors[appErr.FileID] = append(fileErrorsMap.errors[appErr.FileID], appErr)
			fileErrorsMap.mu.Unlock()
		} else if appErr.FileID != -1 {
			// File has too many errors, skip it, and log for manual inspection
			log.Printf("File %d has too many errors, skipping\n", appErr.FileID)
		}
	}
}

// FileStatusWorker updates the final status of each processed file based on whether errors occurred.
func (h *ExtractionHandler) setupDatabase(fileInfoList []models.FileInfo, channels *ExtractionChannels) (func(), *db.FirstWritePartition, error) {
	// Create database tables // TEMP move this into docker setup
	h.dbManager.CreateFileRecordsTable()
	h.dbManager.CreateTradeRecordsTable()

	// Build a set of unique dates from the file information.
	uniqueDates := make(map[time.Time]struct{})
	for _, fileInfo := range fileInfoList {
		normalizedDate := fileInfo.ReferenceDate.Truncate(24 * time.Hour)
		uniqueDates[normalizedDate] = struct{}{}

		// Create a results channel for this date if it doesn't exist yet
		if _, exists := channels.results[normalizedDate]; !exists {
			channels.results[normalizedDate] = make(chan *models.Trade, h.config.resultsChannelSize)
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
	totalDBWorkers := len(uniqueDates) * h.config.numDBWorkersPerReferenceDate
	log.Printf("Creating %d staging tables for %d dates with %d workers per date", totalDBWorkers, len(uniqueDates), h.config.numDBWorkersPerReferenceDate)

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

// fileStatusWorker updates the final status of each processed file based on whether errors occurred.
func (h *ExtractionHandler) fileStatusWorker(fileErrorsMap *FileErrorMap, waitGroups *ExtractionWaitGroups, fileMap *FileMap) {
	defer waitGroups.errorWg.Done()

	fileErrorsMap.mu.Lock()
	defer fileErrorsMap.mu.Unlock()

	for fileID := range *fileMap {
		appErrors := fileErrorsMap.errors[fileID]
		status := db.FILE_STATUS_DONE
		if len(appErrors) > 0 {
			status = db.FILE_STATUS_DONE_WITH_ERRORS
		}

		if err := h.dbManager.UpdateFileStatus(fileID, status, appErrors); err != nil {
			log.Printf("Failed to update status for fileID %d: %v\n", fileID, err)
		}
	}
}

// buildDatesSetAndFiles walks the directory, gets all file paths, and extracts the unique reference dates from them.
// This will allow us to know what dates we will be handling and will serve as a map for idempotency checks.
func (h *ExtractionHandler) buildDatesSetAndFiles(rootPath string) ([]models.FileInfo, error) {
	var fileInfos []models.FileInfo

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err // Propagate errors from walking the path
		}
		if !info.IsDir() {
			// Get the reference date from the file
			refDate, err := parsers.GetReferenceDateFromFile(path)
			if err != nil {
				log.Printf("WARN: Could not get reference date from file %s: %v. Skipping file.", path, err)
				return nil // Skip this file, but continue walking
			}

			fileInfos = append(fileInfos, models.FileInfo{Path: path, ReferenceDate: refDate})
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error walking directory %s: %w", rootPath, err)
	}

	return fileInfos, nil
}

func isPartitionEmpty(partitionName time.Time, partitionMap *db.FirstWritePartition) bool {
	_, exists := (*partitionMap)[partitionName]
	return !exists
}
