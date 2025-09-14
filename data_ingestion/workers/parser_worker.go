package workers

import (
	"fmt"
	"sync"

	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/models"
	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/parsers"
)

// ParserWorker reads file paths from a channel, parses the files, and sends the results to another channel.
func ParserWorker(id int, wg *sync.WaitGroup, jobs <-chan string, results chan<- *models.TradeResult) {
	defer wg.Done()
	for job := range jobs {
		fmt.Printf("Worker %d started job %s\n", id, job)
		err := parsers.ParseCSV(job, results)
		if err != nil {
			fmt.Printf("Error parsing %s: %v\n", job, err)
		}
		fmt.Printf("Worker %d finished job %s\n", id, job)
	}
}
