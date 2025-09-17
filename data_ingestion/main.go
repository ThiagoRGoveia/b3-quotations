package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/db"
	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/handlers"
	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/models"
	"github.com/joho/godotenv"
)

const (
	numParserWorkers   = 7
	numDBWorkers       = 14
	resultsChannelSize = 500000
	dbBatchSize        = 80000
)

func main() {
	startTime := time.Now()
	if len(os.Args) < 2 {
		log.Fatal("Please provide the folder path as a command-line argument.")
	}

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	filesPath := os.Args[1]

	dbpool, err := db.ConnectDB()
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	jobs := make(chan models.FileJob, 100)
	results := make(chan *models.Trade, resultsChannelSize)
	errors := make(chan models.AppError, 100)
	var parserWg, dbWg, errorWg sync.WaitGroup

	ctx := context.Background()
	dbManager := db.NewPostgresDBManager(ctx, dbpool)

	handler := handlers.NewExtractionHandler(dbManager, jobs, results, errors, &parserWg, &dbWg, &errorWg, numParserWorkers, numDBWorkers, dbBatchSize)

	err = handler.Extract(filesPath)
	if err != nil {
		log.Fatalf("Error during extraction: %v\n", err)
		os.Exit(1)
	}

	log.Println("Extraction process finished.")
	log.Printf("Execution time: %s\n", time.Since(startTime))
}
