package main

import (
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DBWorker listens to a channel for Trade data and saves it to the database.
func DBWorker(wg *sync.WaitGroup, trades <-chan *Trade, dbpool *pgxpool.Pool) {
	defer wg.Done()

	for trade := range trades {
		// For now, we'll assume a file_id of 1. This will be updated later.
		_, err := InsertTrade(dbpool, trade, 1, true)
		if err != nil {
			fmt.Printf("Error inserting trade: %v\n", err)
		}
	}
}
