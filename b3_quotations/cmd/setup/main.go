package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/database"
	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("Starting database setup...")

	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable not set")
	}

	dbpool, err := database.ConnectDB(dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbpool.Close()

	dbManager := database.NewPostgresDBManager(context.Background(), dbpool)

	fmt.Println("Creating file_records table...")
	if err := dbManager.CreateFileRecordsTable(); err != nil {
		log.Fatalf("Error creating file_records table: %v", err)
	}
	fmt.Println("file_records table created successfully.")

	fmt.Println("Creating trade_records table...")
	if err := dbManager.CreateTradeRecordsTable(); err != nil {
		log.Fatalf("Error creating trade_records table: %v", err)
	}
	fmt.Println("trade_records table created successfully.")

	fmt.Println("Database setup finished successfully.")
}
