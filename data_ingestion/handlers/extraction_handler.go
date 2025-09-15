package handlers

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/db"
	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/models"
	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/parsers"
)

// ExtractionHandler manages the data extraction and processing workflow.
type ExtractionHandler struct {
	dbManager        db.DBManager
	jobs             chan models.FileJob
	results          chan *models.Trade
	errors           chan models.AppError
	parserWg         *sync.WaitGroup
	dbWg             *sync.WaitGroup
	errorWg          *sync.WaitGroup
	numParserWorkers int
	dbBatchSize      int

	fileErrors   map[int][]string
	fileErrorsMu sync.Mutex
}

// NewExtractionHandler creates a new ExtractionHandler.
func NewExtractionHandler(dbManager db.DBManager, jobs chan models.FileJob, results chan *models.Trade, errors chan models.AppError, parserWg *sync.WaitGroup, dbWg *sync.WaitGroup, errorWg *sync.WaitGroup, numParserWorkers int, dbBatchSize int) *ExtractionHandler {
	return &ExtractionHandler{
		dbManager:        dbManager,
		jobs:             jobs,
		results:          results,
		errors:           errors,
		parserWg:         parserWg,
		dbWg:             dbWg,
		errorWg:          errorWg,
		numParserWorkers: numParserWorkers,
		dbBatchSize:      dbBatchSize,
		fileErrors:       make(map[int][]string),
	}
}

// Extract orchestrates the file processing workflow.
func (h *ExtractionHandler) Extract(filesPath string) error {
	processedFiles := make(map[int]string)

	// Start the error worker
	h.errorWg.Add(1)
	go h.ErrorWorker()

	// Start parser workers
	for w := 1; w <= h.numParserWorkers; w++ {
		h.parserWg.Add(1)
		go h.ParserWorker()
	}

	// Start a single DB worker for batch processing
	h.dbWg.Add(1)
	go h.DBWorker()

	// Goroutine to scan directory and send jobs
	h.DirectoryWorker(filesPath, processedFiles)

	// Wait for all parsing and DB jobs to finish, then close the error channel
	h.parserWg.Wait()
	close(h.results)
	h.dbWg.Wait()
	close(h.errors)

	// Start the file status worker after all file-related processing is done
	h.errorWg.Add(1)
	go h.FileStatusWorker(processedFiles)

	// Wait for the error and status workers to finish
	h.errorWg.Wait()

	fileIDs := make([]int, 0, len(processedFiles))
	for fileID := range processedFiles {
		fileIDs = append(fileIDs, fileID)
	}
	err := h.dbManager.ValidateSavedData(fileIDs)
	if err != nil {
		return fmt.Errorf("error validating saved data: %v", err)
	}
	err = h.dbManager.TransferDataToFinalTable(fileIDs)
	if err != nil {
		return fmt.Errorf("error transferring data to final table: %v", err)
	}
	err = h.dbManager.CleanTempData(fileIDs)
	if err != nil {
		return fmt.Errorf("error cleaning temp data: %v", err)
	}

	log.Println("Extraction process finished.")
	return nil
}

// ParserWorker reads file jobs from a channel, parses the files, and sends the results to another channel.
func (h *ExtractionHandler) ParserWorker() {
	defer h.parserWg.Done()
	for job := range h.jobs {
		log.Printf("Parser worker started job for file %s (ID: %d)\n", job.FilePath, job.FileID)
		err := parsers.ParseCSV(job.FilePath, job.FileID, h.results, h.errors)
		if err != nil {
			h.errors <- models.AppError{FileID: job.FileID, Message: "Failed to open or read file", Err: err}
		}
		log.Printf("Parser worker finished job for file %s (ID: %d)\n", job.FilePath, job.FileID)
	}
}

// DBWorker processes trades from a channel and inserts them into the database in batches.
func (h *ExtractionHandler) DBWorker() {
	defer h.dbWg.Done()
	trades := make([]*models.Trade, 0, h.dbBatchSize)

	for result := range h.results {
		trades = append(trades, result)
		if len(trades) >= h.dbBatchSize {
			err := h.dbManager.InsertMultipleTrades(trades, false)
			if err != nil {
				if len(trades) > 0 {
					h.errors <- models.AppError{FileID: trades[0].FileID, Message: "Failed to insert batch of trades", Err: err}
				}
			}
			trades = trades[:0] // Clear the slice
		}
	}

	// Insert any remaining trades
	if len(trades) > 0 {
		err := h.dbManager.InsertMultipleTrades(trades, false)
		if err != nil {
			if len(trades) > 0 {
				h.errors <- models.AppError{FileID: trades[0].FileID, Message: "Failed to insert remaining batch of trades", Err: err}
			}
		}
	}
}

// ErrorWorker listens on the error channel, logs the errors, and tracks them by FileID.
func (h *ExtractionHandler) ErrorWorker() {
	defer h.errorWg.Done()
	for appErr := range h.errors {
		log.Printf("Caught error: %s\n", appErr.Error())
		// limit the number of errors per file to prevent memory overflow, if more than 100 errors are collected, then file is probably malformed
		if appErr.FileID != -1 && len(h.fileErrors) < 100 {
			h.fileErrorsMu.Lock()
			h.fileErrors[appErr.FileID] = append(h.fileErrors[appErr.FileID], appErr.Error())
			h.fileErrorsMu.Unlock()
		} else if appErr.FileID != -1 {
			// File has too many errors, skip it, and log for manual inspection
			log.Printf("File %d has too many errors, skipping\n", appErr.FileID)
		}
	}
}

// FileStatusWorker updates the final status of each processed file based on whether errors occurred.
func (h *ExtractionHandler) FileStatusWorker(processedFiles map[int]string) {
	defer h.errorWg.Done()

	h.fileErrorsMu.Lock()
	defer h.fileErrorsMu.Unlock()

	for fileID := range processedFiles {
		errors := h.fileErrors[fileID]
		status := db.FILE_STATUS_DONE
		if len(errors) > 0 {
			status = db.FILE_STATUS_DONE_WITH_ERRORS
		}

		if err := h.dbManager.UpdateFileStatus(fileID, status, errors); err != nil {
			log.Printf("Failed to update status for fileID %d: %v\n", fileID, err)
		}
	}
}

func (h *ExtractionHandler) DirectoryWorker(filesPath string, processedFiles map[int]string) {

	defer close(h.jobs)

	filepath.Walk(filesPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			h.errors <- models.AppError{FileID: -1, Message: "Failed to walk directory", Err: err}
			return err
		}
		if !info.IsDir() {
			// Synchronously create file record and get ID
			fileID, err := h.dbManager.InsertFileRecord(path, time.Now(), db.FILE_STATUS_PROCESSING)
			if err != nil {
				log.Printf("Error inserting file record for %s: %v\n", path, err)
				return nil // Continue to next file
			}
			processedFiles[fileID] = path
			h.jobs <- models.FileJob{FilePath: path, FileID: fileID}
		}
		return nil
	})
}
