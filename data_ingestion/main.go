package main

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	numWorkers = 4
	filesPath  = "files"
)

func main() {
	jobs := make(chan string, 100)
	results := make(chan *TradeResult, 100)

	var wg sync.WaitGroup

	// Start workers
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go Worker(w, &wg, jobs, results)
	}

	// Scan directory and send jobs
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

	// Wait for all parsing jobs to be processed, then close the results channel
	go func() {
		wg.Wait()
		close(results)
	}()

	// Setup Database Connection
	dbpool, err := ConnectDB()
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbpool.Close()

	// Process results
	processResults(results, dbpool)
}

func processResults(results <-chan *TradeResult, dbpool *pgxpool.Pool) {
	fileRecords := make(map[string]int)

	for result := range results {
		fileID, exists := fileRecords[result.FilePath]
		if !exists {
			var err error
			fileID, err = InsertFileRecord(dbpool, result.FilePath, time.Now(), "PROCESSING")
			if err != nil {
				log.Printf("Error inserting file record for %s: %v\n", result.FilePath, err)
				continue // Skip this trade if we can't create a file record
			}
			fileRecords[result.FilePath] = fileID
		}

		_, err := InsertTrade(dbpool, result.Trade, fileID, false) // is_valid is false as requested
		if err != nil {
			log.Printf("Error inserting trade for file %s: %v\n", result.FilePath, err)
		}
	}

	// Update status for all processed files
	for path, id := range fileRecords {
		err := UpdateFileStatus(dbpool, id, "DONE")
		if err != nil {
			log.Printf("Error updating file status to DONE for %s: %v\n", path, err)
		}
	}

	log.Println("Finished processing and saving all trades.")
}
