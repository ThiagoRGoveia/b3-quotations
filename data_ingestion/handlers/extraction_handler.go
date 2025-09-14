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
	"github.com/jackc/pgx/v5/pgxpool"
)

// ExtractionHandler manages the data extraction and processing workflow.
type ExtractionHandler struct {
	dbManager        db.DBManager
	dbpool           *pgxpool.Pool
	jobs             chan models.FileJob
	results          chan *models.Trade
	parserWg         *sync.WaitGroup
	dbWg             *sync.WaitGroup
	numParserWorkers int
}

// NewExtractionHandler creates a new ExtractionHandler.
func NewExtractionHandler(dbManager db.DBManager, dbpool *pgxpool.Pool, jobs chan models.FileJob, results chan *models.Trade, parserWg *sync.WaitGroup, dbWg *sync.WaitGroup, numParserWorkers int) *ExtractionHandler {
	return &ExtractionHandler{
		dbManager:        dbManager,
		dbpool:           dbpool,
		jobs:             jobs,
		results:          results,
		parserWg:         parserWg,
		dbWg:             dbWg,
		numParserWorkers: numParserWorkers,
	}
}

// Extract orchestrates the file processing workflow.
func (h *ExtractionHandler) Extract(filesPath string) {
	// Start parser workers
	for w := 1; w <= h.numParserWorkers; w++ {
		h.parserWg.Add(1)
		go h.ParserWorker(w)
	}

	// Start DB workers
	dbWorkerCount := int(h.dbpool.Config().MaxConns)
	for range dbWorkerCount {
		h.dbWg.Add(1)
		go h.DBWorker()
	}

	// Goroutine to scan directory and send jobs
	h.DirectoryWorker(filesPath)

	// Wait for all parsing jobs to finish and then close the results channel
	h.parserWg.Wait()
	close(h.results)

	// Wait for all DB workers to finish
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

// DBWorker processes trades from a channel and inserts them into the database.
func (h *ExtractionHandler) DBWorker() {
	defer h.dbWg.Done()
	for result := range h.results {
		_, err := h.dbManager.InsertTrade(h.dbpool, result, false) // is_valid is false as requested
		if err != nil {
			log.Printf("Error inserting trade: %v\n", err)
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
			fileID, err := h.dbManager.InsertFileRecord(h.dbpool, path, time.Now(), db.FILE_STATUS_PROCESSING)
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
