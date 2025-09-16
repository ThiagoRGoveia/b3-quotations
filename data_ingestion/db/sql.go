package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresDBManager struct {
	dbpool *pgxpool.Pool
}

func NewPostgresDBManager(pool *pgxpool.Pool) *PostgresDBManager {
	return &PostgresDBManager{dbpool: pool}
}

// CreateFileRecordTable creates the file_records table in the database.
func (m *PostgresDBManager) CreateFileRecordsTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS file_records (
		id SERIAL PRIMARY KEY,
		file_name VARCHAR(255) NOT NULL,
		processed_at TIMESTAMP NOT NULL,
		status VARCHAR(50) NOT NULL CHECK (status IN ('DONE', 'DONE_WITH_ERRORS', 'PROCESSING', 'FATAL')),
		errors jsonb
	);`

	_, err := m.dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error creating file_records table: %v", err)
	}

	return nil
}

// CreateTradeRecordsTable creates both the staging and final trade_records tables in the database.
func (m *PostgresDBManager) CreateTradeRecordsTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS trade_records (
		id SERIAL PRIMARY KEY,
		reference_date TIMESTAMP NOT NULL,
		transaction_date TIMESTAMP NOT NULL,
		ticker VARCHAR(255) NOT NULL,
		identifier VARCHAR(255) NOT NULL,
		price NUMERIC(18, 2) NOT NULL,
		quantity BIGINT NOT NULL,
		closing_time VARCHAR(50) NOT NULL,
		file_id INTEGER,
		hash VARCHAR(32) NOT NULL
	);
	`

	_, err := m.dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error creating trade_records table: %v", err)
	}

	return nil
}

func (m *PostgresDBManager) CreateTradeRecordsStagingTable() error {
	query := `
	CREATE UNLOGGED TABLE IF NOT EXISTS trade_records_staging (
		reference_date TIMESTAMP NOT NULL,
		transaction_date TIMESTAMP NOT NULL,
		ticker VARCHAR(255) NOT NULL,
		identifier VARCHAR(255) NOT NULL,
		price NUMERIC(18, 2) NOT NULL,
		quantity BIGINT NOT NULL,
		closing_time VARCHAR(50) NOT NULL,
		file_id INTEGER,
		hash VARCHAR(32) NOT NULL
	);
	`
	_, err := m.dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error creating trade_records_staging table: %v", err)
	}

	return nil
}

func (m *PostgresDBManager) CreateTradeRecordIndexes() error {
	// Create indexes for the main table that will optimize the NOT EXISTS lookups
	queries := []string{
		`CREATE INDEX IF NOT EXISTS idx_trade_records_hash ON trade_records (hash)`,
		`CREATE INDEX IF NOT EXISTS idx_trade_records_ticker_txdate_covering ON trade_records (ticker, transaction_date) INCLUDE (price, quantity)`,
	}

	for _, query := range queries {
		_, err := m.dbpool.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("error creating index: %v", err)
		}
	}

	return nil
}

func (m *PostgresDBManager) DropTradeRecordIndexes() error {
	queries := []string{
		`DROP INDEX IF EXISTS idx_trade_records_hash`,
		`DROP INDEX IF EXISTS idx_trade_records_ticker_txdate_covering`,
	}

	for _, query := range queries {
		_, err := m.dbpool.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("error dropping index: %v", err)
		}
	}

	return nil
}

// InsertFileRecord inserts a new file record into the file_records table.
func (m *PostgresDBManager) InsertFileRecord(fileName string, date time.Time, status string) (int, error) {
	query := `
	INSERT INTO file_records (file_name, processed_at, status)
	VALUES ($1, $2, $3)
	RETURNING id;`

	var fileID int
	err := m.dbpool.QueryRow(context.Background(), query, fileName, date, status).Scan(&fileID)
	if err != nil {
		return 0, fmt.Errorf("error inserting file record: %v", err)
	}

	return fileID, nil
}

// UpdateFileStatus updates the status of a file record in the database.
func (m *PostgresDBManager) UpdateFileStatus(fileID int, status string, errors any) error {
	query := `
	UPDATE file_records
	SET status = $1,
		errors = $2
	WHERE id = $3;`

	_, err := m.dbpool.Exec(context.Background(), query, status, errors, fileID)
	if err != nil {
		return fmt.Errorf("error updating file status: %v", err)
	}

	return nil
}

// InsertMultipleTrades inserts multiple trade records using the bulk load, filter, and insert pattern.
func (m *PostgresDBManager) InsertMultipleTrades(trades []*models.Trade) error {
	// Step 1: Bulk load into the staging table for maximum speed
	log.Println("Bulk loading trades into staging table...")
	_, err := m.dbpool.CopyFrom(
		context.Background(),
		pgx.Identifier{"trade_records_staging"},
		[]string{"hash", "reference_date", "transaction_date", "ticker", "identifier", "price", "quantity", "closing_time", "file_id"},
		pgx.CopyFromSlice(len(trades), func(i int) ([]any, error) {
			trade := trades[i]
			return []any{trade.Hash, trade.ReferenceDate, trade.TransactionDate, trade.Ticker, trade.Identifier, trade.Price, trade.Quantity, trade.ClosingTime, trade.FileID}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("unable to copy trades to staging table: %v", err)
	}

	// Step 2: Insert by exclusion - only insert records that don't already exist
	insertQuery := `
	INSERT INTO trade_records (hash, reference_date, transaction_date, ticker, identifier, price, quantity, closing_time, file_id)
	SELECT s.hash, s.reference_date, s.transaction_date, s.ticker, s.identifier, s.price, s.quantity, s.closing_time, s.file_id
	FROM trade_records_staging s
	WHERE NOT EXISTS (
		SELECT 1
		FROM trade_records t
		WHERE t.hash = s.hash
	);
	`

	log.Println("Inserting from staging to main table.")
	_, err = m.dbpool.Exec(context.Background(), insertQuery)
	if err != nil {
		return fmt.Errorf("error inserting from staging to main table: %v", err)
	}

	// Step 3: Truncate the staging table to prepare for the next batch
	truncateQuery := `TRUNCATE trade_records_staging;`
	log.Println("Truncate staging table.")
	_, err = m.dbpool.Exec(context.Background(), truncateQuery)
	if err != nil {
		return fmt.Errorf("error truncating staging table: %v", err)
	}

	return nil
}
