package main

import (
	"log"
	"os"
	"sync"

	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/db"
	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/handlers"
	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/models"
)

const (
	numParserWorkers = 4
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Please provide the folder path as a command-line argument.")
	}
	filesPath := os.Args[1]
	dbpool, err := db.ConnectDB()
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	jobs := make(chan models.FileJob, 100)
	results := make(chan *models.Trade, 1000)
	errors := make(chan models.AppError, 100)
	var parserWg, dbWg, errorWg sync.WaitGroup

	dbManager := db.NewPostgresDBManager(dbpool)

	if err := dbManager.CreateFileRecordTable(); err != nil {
		log.Fatalf("Unable to create file_records table: %v\n", err)
	}
	if err := dbManager.CreateTradeLoadedRecordTable(); err != nil {
		log.Fatalf("Unable to create trade_loaded_records table: %v\n", err)
	}

	handler := handlers.NewExtractionHandler(dbManager, jobs, results, errors, &parserWg, &dbWg, &errorWg, numParserWorkers, 100)

	err = handler.Extract(filesPath)
	if err != nil {
		log.Fatalf("Error during extraction: %v\n", err)
		os.Exit(1)
	}

	log.Println("Extraction process finished.")
}
