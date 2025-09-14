package main

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/db"
	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/models"
	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/workers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	numWorkers = 4
	filesPath  = "files"
)

func processResults(results <-chan *models.TradeResult, dbpool *pgxpool.Pool) {
	fileRecords := make(map[string]int)

	for result := range results {
		fileID, exists := fileRecords[result.FilePath]
		if !exists {
			var err error
			fileID, err = db.InsertFileRecord(dbpool, result.FilePath, time.Now(), "PROCESSING")
			if err != nil {
				log.Printf("Error inserting file record for %s: %v\n", result.FilePath, err)
				continue // Skip this trade if we can't create a file record
			}
			fileRecords[result.FilePath] = fileID
		}

		_, err := db.InsertTrade(dbpool, result.Trade, fileID, false) // is_valid is false as requested
		if err != nil {
			log.Printf("Error inserting trade for file %s: %v\n", result.FilePath, err)
		}
	}

	// Update status for all processed files
	for path, id := range fileRecords {
		err := db.UpdateFileStatus(dbpool, id, "DONE")
		if err != nil {
			log.Printf("Error updating file status to DONE for %s: %v\n", path, err)
		}
	}

	log.Println("Finished processing and saving all trades.")
}

func setup() (*pgxpool.Pool, chan string, chan *models.TradeResult, *sync.WaitGroup) {
	log.Println("Starting setup...")

	dbpool, err := db.ConnectDB()
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}

	jobs := make(chan string, 100)
	results := make(chan *models.TradeResult, 100)
	var wg sync.WaitGroup

	// Start workers
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go workers.ParserWorker(w, &wg, jobs, results)
	}

	log.Println("Setup complete.")
	return dbpool, jobs, results, &wg
}

func execute(dbpool *pgxpool.Pool, jobs chan<- string, results chan *models.TradeResult, wg *sync.WaitGroup) {
	log.Println("Executing process...")

	// Goroutine to scan directory and send jobs
	go func() {
		defer close(jobs)
		err := filepath.Walk(filesPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				jobs <- path
			}
			return nil
		})
		if err != nil {
			log.Printf("Error walking the path %s: %v", filesPath, err)
		}
	}()

	// Goroutine to wait for all parsing jobs to finish and then close the results channel
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process the results from the channel
	processResults(results, dbpool)

	log.Println("Execution complete.")
}

func teardown(dbpool *pgxpool.Pool) {
	log.Println("Tearing down...")
	dbpool.Close()
	log.Println("Teardown complete.")
}

func main() {
	dbpool, jobs, results, wg := setup()
	execute(dbpool, jobs, results, wg)
	teardown(dbpool)
}
