package db

import (
	"context"
	"fmt"
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
		reference_date TIMESTAMP NOT NULL,
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

// CreateTradeRecordsTable creates the trade_records table in the database.
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
);`

	_, err := m.dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error creating trade_records table: %v", err)
	}

	return nil
}

func (m *PostgresDBManager) CreateTradeRecordIndexes() error {
	query := `
	CREATE INDEX idx_trade_records_covering ON trade_records (ticker, transaction_date, price, quantity);`

	_, err := m.dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error creating index: %v", err)
	}

	return nil
}

func (m *PostgresDBManager) DropTradeRecordIndexes() error {
	query := `
	DROP INDEX idx_trade_records_covering;`

	_, err := m.dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error dropping index: %v", err)
	}

	return nil
}

// InsertFileRecord inserts a new file record into the file_records table.
func (m *PostgresDBManager) InsertFileRecord(fileName string, date time.Time, status string) (int, error) {
	query := `
	INSERT INTO file_records (file_name, reference_date, status)
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

// InsertMultipleTrades inserts multiple trade records in a single transaction.
func (m *PostgresDBManager) InsertMultipleTrades(trades []*models.Trade) error {
	_, err := m.dbpool.CopyFrom(
		context.Background(),
		pgx.Identifier{"trade_records"},
		[]string{"hash", "reference_date", "transaction_date", "ticker", "identifier", "price", "quantity", "closing_time", "file_id"},
		pgx.CopyFromSlice(len(trades), func(i int) ([]any, error) {
			trade := trades[i]
			return []any{trade.Hash, trade.ReferenceDate, trade.TransactionDate, trade.Ticker, trade.Identifier, trade.Price, trade.Quantity, trade.ClosingTime, trade.FileID}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("unable to copy trades from slice: %v", err)
	}

	return nil
}
