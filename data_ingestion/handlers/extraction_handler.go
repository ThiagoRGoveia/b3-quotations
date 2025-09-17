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

type FileErrorMap struct {
	errors map[int][]models.AppError
	mu     sync.Mutex
}

type ExtractionChannels struct {
	results chan *models.Trade
	errors  chan models.AppError
	jobs    chan models.FileProcessingJob
}

type ExtractionWaitGroups struct {
	parserWg *sync.WaitGroup
	dbWg     *sync.WaitGroup
	errorWg  *sync.WaitGroup
}

type Config struct {
	numParserWorkers   int
	dbBatchSize        int
	numDBWorkers       int
	resultsChannelSize int
}

type ExtractionHandler struct {
	dbManager db.DBManager
	config    Config
}

// FileMap is a map of file IDs to their paths.
type FileMap = map[int]string

// SetupExtractionHandler creates a new ExtractionHandler with all necessary channels and waitgroups.
func SetupExtractionHandler(dbManager db.DBManager, config Config) *ExtractionHandler {
	// Create channels

	return &ExtractionHandler{
		dbManager: dbManager,
		config:    config,
	}
}

// NewExtractionHandler creates a new ExtractionHandler with externally managed channels and waitgroups.
// This function is kept for backward compatibility.
func NewExtractionHandler(dbManager db.DBManager, numParserWorkers int, numDBWorkers int, dbBatchSize int, resultsChannelSize int) *ExtractionHandler {
	return &ExtractionHandler{
		dbManager: dbManager,
		config: Config{
			numParserWorkers:   numParserWorkers,
			numDBWorkers:       numDBWorkers,
			dbBatchSize:        dbBatchSize,
			resultsChannelSize: resultsChannelSize,
		},
	}
}

func (h *ExtractionHandler) Setup() (*ExtractionChannels, *ExtractionWaitGroups, *FileMap, *FileErrorMap) {
	results := make(chan *models.Trade, h.config.resultsChannelSize)
	jobs := make(chan models.FileProcessingJob, 100)
	errors := make(chan models.AppError, 100)
	var parserWg, dbWg, errorWg sync.WaitGroup
	fileMap := make(map[int]string)
	fileErrorsMap := FileErrorMap{errors: make(map[int][]models.AppError)}
	return &ExtractionChannels{results: results, errors: errors, jobs: jobs}, &ExtractionWaitGroups{parserWg: &parserWg, dbWg: &dbWg, errorWg: &errorWg}, &fileMap, &fileErrorsMap
}

// Execute orchestrates the file processing workflow.
func (h *ExtractionHandler) Execute(filesPath string) error {
	// Step 0: Setup channels, waitgroups, file map, and error map.
	channels, waitGroups, fileMap, fileErrorsMap := h.Setup()
	// Step 1: Synchronously get all file paths and their reference dates.
	log.Println("Scanning files to determine required partitions...")
	fileInfo, err := h.buildDatesSetAndFiles(filesPath)
	if err != nil {
		log.Fatalf("Failed to scan files: %v", err)
		panic(err)
	}

	// Step 2: Setup the database and get the cleanup function.
	cleanup := h.setupDatabase(fileInfo)
	defer cleanup()
	log.Println("Database setup complete. All necessary tables and partitions are ready.")

	// Step 3: Preprocess files.
	log.Println("Preprocessing files...")
	h.PreprocessFile(fileInfo, fileMap, channels)

	// Step 4: Start all worker pools.
	log.Println("Starting worker pools...")
	h.startWorkers(channels, fileErrorsMap, waitGroups)

	// Step 5: Wait for all processing to complete.
	log.Println("Waiting for all workers to finish...")
	waitGroups.parserWg.Wait()
	close(channels.results)
	close(channels.jobs)
	waitGroups.dbWg.Wait()
	close(channels.errors)

	// Start the file status worker after all file-related processing is done
	waitGroups.errorWg.Add(1)
	go h.FileStatusWorker(channels, fileErrorsMap, waitGroups, fileMap)

	// Wait for the error and status workers to finish
	waitGroups.errorWg.Wait()

	log.Println("Extraction process finished.")
	return nil
}

func (h *ExtractionHandler) PreprocessFile(fileInfo []models.FileInfo, fileMap *FileMap, channels *ExtractionChannels) {
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

// ParserWorker reads file jobs from a channel, parses the files, and sends the results to another channel.
func (h *ExtractionHandler) ParserWorker(channels *ExtractionChannels, fileErrorsMap *FileErrorMap, waitGroups *ExtractionWaitGroups) {
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

// DBWorker processes trades from a channel and inserts them into the database in batches.
func (h *ExtractionHandler) DBWorker(workerId int, stagingTableName string, channels *ExtractionChannels, waitGroups *ExtractionWaitGroups) {
	defer waitGroups.dbWg.Done()
	trades := make([]*models.Trade, 0, h.config.dbBatchSize)

	for result := range channels.results {
		trades = append(trades, result)
		if len(trades) >= h.config.dbBatchSize {
			log.Printf("DB Worker %d: Inserting batch of %d trades using table %s\n", workerId, len(trades), stagingTableName)
			// Using the DB manager's stored context
			err := h.dbManager.InsertMultipleTrades(trades, stagingTableName)
			if err != nil {
				// The batch failed, so report an error for each unique FileID in the batch.
				// Maybe log the trades that failed? A next step here is retry logic but since
				// the error here suggests network or database connection issues, it's better
				// to report the error and let the user handle it.
				fileIDs := make(map[int]bool)
				for _, trade := range trades {
					fileIDs[trade.FileID] = true
				}
				for fileID := range fileIDs {
					channels.errors <- models.AppError{FileID: fileID, Message: "Failed to insert batch of trades", Err: err}
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
				channels.errors <- models.AppError{FileID: fileID, Message: "Failed to insert remaining batch of trades", Err: err}
			}
		}
	}

	log.Printf("DB worker %d finished.", workerId)
}

// ErrorWorker listens on the error channel, logs the errors, and tracks them by FileID.
func (h *ExtractionHandler) ErrorWorker(channels *ExtractionChannels, fileErrorsMap *FileErrorMap, waitGroups *ExtractionWaitGroups) {
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
func (h *ExtractionHandler) setupDatabase(fileInfoList []models.FileInfo) func() {
	// Create database tables
	h.dbManager.CreateFileRecordsTable()
	h.dbManager.CreateTradeRecordsTable()

	// Build a set of unique dates from the file information.
	uniqueDates := make(map[time.Time]struct{})
	for _, fileInfo := range fileInfoList {
		normalizedDate := fileInfo.ReferenceDate.Truncate(24 * time.Hour)
		uniqueDates[normalizedDate] = struct{}{}
	}
	log.Printf("Found %d unique dates. Ensuring partitions exist...", len(uniqueDates))

	for date := range uniqueDates {
		exists, err := h.dbManager.CheckIfPartitionExists(date)
		if err != nil {
			log.Fatalf("Failed to check for partition for date %s: %v", date.Format("2006-01-02"), err)
			panic(err) // Fatal cannot continue
		}

		if !exists {
			if err := h.dbManager.CreatePartitionForDate(date); err != nil {
				// If partition creation fails, it's a fatal error as ingestion will fail.
				log.Fatalf("Failed to create partition for date %s: %v", date.Format("2006-01-02"), err)
				panic(err) // Fatal cannot continue
			}
		} else {
			log.Printf("Partition for date %s already exists. Skipping creation.", date.Format("2006-01-02"))
		}
	}

	// Create staging tables for each DB worker and collect their names for cleanup.
	var stagingTableNames []string
	for w := 1; w <= h.config.numDBWorkers; w++ {
		stagingTableName := fmt.Sprintf("trade_records_staging_worker_%d", w)
		if err := h.dbManager.CreateWorkerStagingTable(stagingTableName); err != nil {
			log.Fatalf("Failed to create staging table for worker %d: %v", w, err)
			panic(err) // Fatal cannot continue
		}
		stagingTableNames = append(stagingTableNames, stagingTableName)
	}

	// Return a cleanup function to be deferred by the caller.
	return func() {
		for _, tableName := range stagingTableNames {
			log.Printf("Cleaning up staging table %s", tableName)
			h.dbManager.DropWorkerStagingTable(tableName)
		}
	}
}

// FileStatusWorker updates the final status of each processed file based on whether errors occurred.
func (h *ExtractionHandler) FileStatusWorker(channels *ExtractionChannels, fileErrorsMap *FileErrorMap, waitGroups *ExtractionWaitGroups, fileMap *FileMap) {
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

// startWorkers initializes and starts all the worker pools (parser, DB, error).
func (h *ExtractionHandler) startWorkers(channels *ExtractionChannels, fileErrorsMap *FileErrorMap, waitGroups *ExtractionWaitGroups) {
	// Start the error worker
	waitGroups.errorWg.Add(1)
	go h.ErrorWorker(channels, fileErrorsMap, waitGroups)

	// Start parser workers
	for w := 1; w <= h.config.numParserWorkers; w++ {
		waitGroups.parserWg.Add(1)
		go h.ParserWorker(channels, fileErrorsMap, waitGroups)
	}

	// Start DB workers
	for w := 1; w <= h.config.numDBWorkers; w++ {
		workerId := w
		stagingTableName := fmt.Sprintf("trade_records_staging_worker_%d", workerId)
		// The staging table is already created in the Extract method.
		waitGroups.dbWg.Add(1)
		go h.DBWorker(w, stagingTableName, channels, waitGroups)
	}
}
