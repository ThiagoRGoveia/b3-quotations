package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/thiago-r-goveia/b3-quotations/api/handlers/db"
	"github.com/thiago-r-goveia/b3-quotations/api/handlers/handlers"
	"github.com/thiago-r-goveia/b3-quotations/api/handlers/routes"
)

func main() {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatal("DATABASE_URL environment variable is not set")
	}

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}

	dbManager, err := db.NewPostgresTickerDBManager(context.Background(), connStr)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer dbManager.Close()

	tickerHandler := handlers.NewTickerHandler(dbManager)
	router := routes.SetupRoutes(tickerHandler)

	log.Printf("Server starting on port %s", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%s", port), router); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
