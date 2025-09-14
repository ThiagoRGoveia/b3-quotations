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

	// The DBManager is implicitly satisfied by the db package functions,
	// so we can pass nil for the interface if we don't have a struct that implements it.
	var dbManager db.DBManager

	handler := handlers.NewExtractionHandler(dbManager, dbpool, jobs, results, &parserWg, &dbWg, numParserWorkers)

	handler.Extract(filesPath)
}
