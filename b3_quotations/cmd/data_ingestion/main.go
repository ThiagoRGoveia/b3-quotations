package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/config"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/database"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/ingestion"
	"github.com/joho/godotenv"
)

func setup() (string, *ingestion.IngestionService, func(), error) {
	if len(os.Args) < 2 {
		return "", nil, nil, fmt.Errorf("please provide the folder path as a command-line argument")
	}
	filesPath := os.Args[1]

	cfg, err := config.New()
	if err != nil {
		return "", nil, nil, fmt.Errorf("failed to load config: %w", err)
	}

	dbpool, err := database.ConnectDB(cfg.DatabaseURL)
	if err != nil {
		return "", nil, nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	ctx := context.Background()
	dbManager := database.NewPostgresDBManager(ctx, dbpool)

	fileProcessor := ingestion.NewFileProcessor(dbManager)
	asyncWorker := ingestion.NewAsyncWorker(dbManager, ingestion.AsyncWorkerConfig{
		NumDBWorkersPerReferenceDate: cfg.NumDBWorkersPerReferenceDate,
		DBBatchSize:                  cfg.DBBatchSize,
	})

	handler := ingestion.NewIngestionService(
		dbManager,
		asyncWorker,
		fileProcessor,
		*cfg,
	)

	cleanupFunc := func() {
		dbpool.Close()
	}

	return filesPath, handler, cleanupFunc, nil
}

func execute(filesPath string, handler *ingestion.IngestionService) error {
	log.Println("Starting extraction process...")
	return handler.Execute(filesPath)
}

func cleanup(cleanupFunc func()) {
	log.Println("Cleaning up resources...")
	cleanupFunc()
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: could not load .env file: %v", err)
	}
	startTime := time.Now()

	filesPath, handler, cleanupFunc, err := setup()
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup(cleanupFunc)

	err = execute(filesPath, handler)
	if err != nil {
		log.Fatalf("Error during extraction: %v\n", err)
	}

	log.Println("Extraction process finished.")
	log.Printf("Execution time: %s\n", time.Since(startTime))
}
