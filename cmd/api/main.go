package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/database"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/server"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatal("DATABASE_URL environment variable is not set")
	}

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}

	dbpool, err := database.ConnectDB(connStr)
	ctx := context.Background()
	dbManager := database.NewPostgresDBManager(ctx, dbpool)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer dbpool.Close()

	if err != nil {
		log.Fatalf("Failed to setup database: %v", err)
	}

	router := server.SetupRoutes(server.NewTickerService(dbManager))

	log.Printf("Server starting on port %s", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%s", port), router); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
