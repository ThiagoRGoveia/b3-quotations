package handlers

import (
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
	parserWg         *sync.WaitGroup
	dbWg             *sync.WaitGroup
	numParserWorkers int
	dbBatchSize      int
}

// NewExtractionHandler creates a new ExtractionHandler.
func NewExtractionHandler(dbManager db.DBManager, jobs chan models.FileJob, results chan *models.Trade, parserWg *sync.WaitGroup, dbWg *sync.WaitGroup, numParserWorkers int, dbBatchSize int) *ExtractionHandler {
	return &ExtractionHandler{
		dbManager:        dbManager,
		jobs:             jobs,
		results:          results,
		parserWg:         parserWg,
		dbWg:             dbWg,
		numParserWorkers: numParserWorkers,
		dbBatchSize:      dbBatchSize,
	}
}

// Extract orchestrates the file processing workflow.
func (h *ExtractionHandler) Extract(filesPath string) {
	// Start parser workers
	for w := 1; w <= h.numParserWorkers; w++ {
		h.parserWg.Add(1)
		go h.ParserWorker(w)
	}

	// Start a single DB worker for batch processing
	h.dbWg.Add(1)
	go h.DBWorker()

	// Goroutine to scan directory and send jobs
	h.DirectoryWorker(filesPath)

	// Wait for all parsing jobs to finish and then close the results channel
	h.parserWg.Wait()
	close(h.results)

	// Wait for the DB worker to finish
	h.dbWg.Wait()

	log.Println("Extraction process initiated.")
}

// ParserWorker reads file jobs from a channel, parses the files, and sends the results to another channel.
func (h *ExtractionHandler) ParserWorker(id int) {
	defer h.parserWg.Done()
	for job := range h.jobs {
		log.Printf("Parser worker %d started job for file %s\n", id, job.FilePath)
		err := parsers.ParseCSV(job.FilePath, job.FileID, h.results)
		if err != nil {
			log.Printf("Error parsing %s: %v\n", job.FilePath, err)
		}
		log.Printf("Parser worker %d finished job for file %s\n", id, job.FilePath)
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
				log.Printf("Error inserting batch of trades: %v\n", err)
			}
			trades = trades[:0] // Clear the slice
		}
	}

	// Insert any remaining trades
	if len(trades) > 0 {
		err := h.dbManager.InsertMultipleTrades(trades, false)
		if err != nil {
			log.Printf("Error inserting remaining batch of trades: %v\n", err)
		}
	}
}

func (h *ExtractionHandler) DirectoryWorker(filesPath string) {

	defer close(h.jobs)
	processedFiles := make(map[int]string)

	filepath.Walk(filesPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
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
