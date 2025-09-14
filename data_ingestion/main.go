package main

import (
	"fmt"
	"log"

	// "os"
	// "path/filepath"
	"sync"
)

const (
	numWorkers = 4
	// filesPath     = "files"
)

func main() {
	jobs := make(chan string, 100)
	results := make(chan *Trade, 100)

	var wg sync.WaitGroup

	// Start workers
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go Worker(w, &wg, jobs, results)
	}

	// Send the specific file to the jobs channel
	go func() {
		// filepath.Walk(filesPath, func(path string, info os.FileInfo, err error) error {
		// 	if err != nil {
		// 		return err
		// 	}
		// 	if !info.IsDir() {
		// 		jobs <- path
		// 	}
		// 	return nil
		// })
		jobs <- "files/sample.txt"
		close(jobs)
	}()

	// Wait for all jobs to be processed
	go func() {
		wg.Wait()
		close(results)
	}()

	// TESTING Collect results
	var allTrades []*Trade
	for r := range results {
		allTrades = append(allTrades, r)
	}

	log.Printf("Processed %d trades", len(allTrades))

	// Print some trades to verify
	for i, trade := range allTrades {
		if i > 10 {
			break
		}
		fmt.Printf("%+v\n", trade)
	}
}
