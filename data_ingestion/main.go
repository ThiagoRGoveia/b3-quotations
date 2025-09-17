package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/db"
	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/handlers"
	"github.com/joho/godotenv"
)

const (
	numParserWorkers             = 7
	numDBWorkersPerReferenceDate = 2
	resultsChannelSize           = 500000
	dbBatchSize                  = 80000
)

// setup initializes resources and returns handler and cleanup function
func setup() (string, *handlers.ExtractionHandler, func(), error) {
	// Validate command-line arguments
	if len(os.Args) < 2 {
		return "", nil, nil, fmt.Errorf("please provide the folder path as a command-line argument")
	}
	filesPath := os.Args[1]

	// Load environment variables
	if err := godotenv.Load(); err != nil {
		return "", nil, nil, fmt.Errorf("error loading .env file: %w", err)
	}

	// Connect to the database
	dbpool, err := db.ConnectDB()
	if err != nil {
		return "", nil, nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	// Create database manager
	ctx := context.Background()
	dbManager := db.NewPostgresDBManager(ctx, dbpool)

	// Create extraction handler
	handler := handlers.NewExtractionHandler(
		dbManager,
		numParserWorkers,
		numDBWorkersPerReferenceDate,
		dbBatchSize,
		resultsChannelSize,
	)

	// Define cleanup function
	cleanupFunc := func() {
		dbpool.Close()
	}

	return filesPath, handler, cleanupFunc, nil
}

// execute runs the main extraction process
func execute(filesPath string, handler *handlers.ExtractionHandler) error {
	log.Println("Starting extraction process...")
	return handler.Execute(filesPath)
}

// cleanup releases resources
func cleanup(cleanupFunc func()) {
	log.Println("Cleaning up resources...")
	cleanupFunc()
}

func main() {
	startTime := time.Now()

	// Setup phase
	filesPath, handler, cleanupFunc, err := setup()
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup(cleanupFunc)

	// Execute phase
	err = execute(filesPath, handler)
	if err != nil {
		log.Fatalf("Error during extraction: %v\n", err)
	}

	log.Println("Extraction process finished.")
	log.Printf("Execution time: %s\n", time.Since(startTime))
}
