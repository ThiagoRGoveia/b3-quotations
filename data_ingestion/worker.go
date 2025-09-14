package main

import (
	"fmt"
	"sync"
)

// Worker reads file paths from a channel, parses the files, and sends the results to another channel.
func Worker(id int, wg *sync.WaitGroup, jobs <-chan string, results chan<- *Trade) {
	defer wg.Done()
	for job := range jobs {
		fmt.Printf("Worker %d started job %s\n", id, job)
		err := ParseCSV(job, results)
		if err != nil {
			fmt.Printf("Error parsing %s: %v\n", job, err)
		}
		fmt.Printf("Worker %d finished job %s\n", id, job)
	}
}
