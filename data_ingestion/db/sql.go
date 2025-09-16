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

// CreateTradeRecordsTable creates the final trade_records table in the database.
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

// ADD: A new function to create a uniquely named UNLOGGED staging table for a worker.
func (m *PostgresDBManager) CreateWorkerStagingTable(tableName string) error {
	// We use LIKE to ensure the structure always matches the target table.
	query := fmt.Sprintf(`CREATE UNLOGGED TABLE IF NOT EXISTS %s (LIKE trade_records INCLUDING DEFAULTS);`, pgx.Identifier{tableName}.Sanitize())
	_, err := m.dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error creating worker staging table %s: %v", tableName, err)
	}
	return nil
}

// ADD: A new function to drop the worker's staging table during cleanup.
func (m *PostgresDBManager) DropWorkerStagingTable(tableName string) error {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, pgx.Identifier{tableName}.Sanitize())
	_, err := m.dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error dropping worker staging table %s: %v", tableName, err)
	}
	return nil
}

func (m *PostgresDBManager) CreateTradeRecordIndexes() error {
	// Create indexes for the main table that will optimize the NOT EXISTS lookups
	queries := []string{
		`CREATE INDEX IF NOT EXISTS idx_trade_records_hash ON trade_records (reference_date, hash)`,
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
func (m *PostgresDBManager) InsertMultipleTrades(ctx context.Context, trades []*models.Trade, stagingTableName string) error {
	// Step 1: Bulk load into the worker's assigned staging table.
	log.Printf("Bulk loading %d trades into staging table %s", len(trades), stagingTableName)
	_, err := m.dbpool.CopyFrom(
		ctx,
		pgx.Identifier{stagingTableName}, // Use the passed-in table name.
		[]string{"hash", "reference_date", "transaction_date", "ticker", "identifier", "price", "quantity", "closing_time", "file_id"},
		pgx.CopyFromSlice(len(trades), func(i int) ([]any, error) {
			trade := trades[i]
			return []any{trade.Hash, trade.ReferenceDate, trade.TransactionDate, trade.Ticker, trade.Identifier, trade.Price, trade.Quantity, trade.ClosingTime, trade.FileID}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("unable to copy trades to staging table %s: %v", stagingTableName, err)
	}

	// Step 2: Insert by exclusion, reading from the worker's assigned staging table.
	// CRITICAL FIX: The WHERE clause uses the correct composite key for idempotency.
	insertQuery := fmt.Sprintf(`
	INSERT INTO trade_records (hash, reference_date, transaction_date, ticker, identifier, price, quantity, closing_time, file_id)
	SELECT s.hash, s.reference_date, s.transaction_date, s.ticker, s.identifier, s.price, s.quantity, s.closing_time, s.file_id
	FROM %s s
	WHERE NOT EXISTS (
		SELECT 1
		FROM trade_records t
		WHERE t.reference_date = s.reference_date AND t.hash = s.hash
	);
	`, pgx.Identifier{stagingTableName}.Sanitize())

	log.Printf("Inserting from staging table %s to main table.", stagingTableName)
	_, err = m.dbpool.Exec(ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("error inserting from staging table %s: %v", stagingTableName, err)
	}

	// Step 3: Truncate the worker's staging table to prepare for its *next* batch.
	truncateQuery := fmt.Sprintf(`TRUNCATE %s;`, pgx.Identifier{stagingTableName}.Sanitize())
	log.Printf("Truncating staging table %s.", stagingTableName)
	_, err = m.dbpool.Exec(ctx, truncateQuery)
	if err != nil {
		// This is not a fatal error for the data itself, but should be logged as a warning.
		log.Printf("WARN: failed to truncate staging table %s: %v", stagingTableName, err)
	}

	return nil
}
