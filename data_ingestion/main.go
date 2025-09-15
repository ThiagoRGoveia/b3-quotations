package main

import (
	"log"
	"sync"

	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/db"
	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/handlers"
	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/models"
)

const (
	numParserWorkers = 4
	filesPath        = "files"
)

func main() {
	dbpool, err := db.ConnectDB()
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbpool.Close()

	jobs := make(chan models.FileJob, 100)
	results := make(chan *models.Trade, 100)
	var parserWg, dbWg sync.WaitGroup

	var dbManager db.DBManager

	handler := handlers.NewExtractionHandler(dbManager, dbpool, jobs, results, &parserWg, &dbWg, numParserWorkers, 100)

	handler.Extract(filesPath)
}
