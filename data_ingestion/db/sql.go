package db

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresDBManager struct {
	dbpool *pgxpool.Pool
	ctx    context.Context
}

func NewPostgresDBManager(ctx context.Context, pool *pgxpool.Pool) *PostgresDBManager {
	return &PostgresDBManager{dbpool: pool, ctx: ctx}
}

// CreateFileRecordTable creates the file_records table in the database.
func (m *PostgresDBManager) CreateFileRecordsTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS file_records (
		id SERIAL PRIMARY KEY,
		file_name VARCHAR(255) NOT NULL,
		processed_at TIMESTAMP NOT NULL,
		status VARCHAR(50) NOT NULL CHECK (status IN ('DONE', 'DONE_WITH_ERRORS', 'PROCESSING', 'FATAL')),
		checksum VARCHAR(64),
		reference_date TIMESTAMP,
		errors jsonb
	);`

	_, err := m.dbpool.Exec(m.ctx, query)
	if err != nil {
		return fmt.Errorf("error creating file_records table: %v", err)
	}

	return nil
}

// CreateTradeRecordsTable creates the final trade_records parent partition table.
func (m *PostgresDBManager) CreateTradeRecordsTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS trade_records (
		id BIGSERIAL NOT NULL, 
		reference_date TIMESTAMP NOT NULL,
		transaction_date TIMESTAMP NOT NULL,
		ticker VARCHAR(255) NOT NULL,
		identifier VARCHAR(255) NOT NULL,
		price NUMERIC(18, 2) NOT NULL,
		quantity BIGINT NOT NULL,
		closing_time VARCHAR(50) NOT NULL,
		file_id INTEGER,
		hash VARCHAR(32) NOT NULL
	) PARTITION BY RANGE (reference_date); 
	`
	// Note: We intentionally avoid creating global unique indexes (like PRIMARY KEY)
	// on the parent table. Uniqueness (reference_date, hash) will be enforced on child partitions.

	_, err := m.dbpool.Exec(m.ctx, query)
	if err != nil {
		return fmt.Errorf("error creating trade_records partition table: %v", err)
	}

	return nil
}

// ADD: A new function to create a uniquely named UNLOGGED staging table for a worker.
func (m *PostgresDBManager) CreateWorkerStagingTable(tableName string) error {
	// We use LIKE to ensure the structure always matches the target table.
	query := fmt.Sprintf(`CREATE UNLOGGED TABLE IF NOT EXISTS %s (LIKE trade_records INCLUDING DEFAULTS);`, pgx.Identifier{tableName}.Sanitize())
	_, err := m.dbpool.Exec(m.ctx, query)
	if err != nil {
		return fmt.Errorf("error creating worker staging table %s: %v", tableName, err)
	}
	return nil
}

// ADD: A new function to drop the worker's staging table during cleanup.
func (m *PostgresDBManager) DropWorkerStagingTable(tableName string) error {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, pgx.Identifier{tableName}.Sanitize())
	_, err := m.dbpool.Exec(m.ctx, query)
	if err != nil {
		return fmt.Errorf("error dropping worker staging table %s: %v", tableName, err)
	}
	return nil
}

func (m *PostgresDBManager) CreateTradeRecordIndexes() error {
	// Create index on the parent table. This index will be automatically
	// created on all child partitions, optimizing the WHERE NOT EXISTS lookup.
	queries := []string{
		// Note: B-Tree index on a partitioned table is automatically a partitioned index.
		`CREATE INDEX IF NOT EXISTS idx_trade_records_hash ON trade_records (reference_date, hash)`,
	}

	for _, query := range queries {
		_, err := m.dbpool.Exec(m.ctx, query)
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
		_, err := m.dbpool.Exec(m.ctx, query)
		if err != nil {
			return fmt.Errorf("error dropping index: %v", err)
		}
	}

	return nil
}

// getPartitionTableName generates a consistent partition name for a given date.
func getPartitionTableName(date time.Time) string {
	// Format: trade_records_YYYYMMDD
	return fmt.Sprintf("trade_records_%s", date.Format("20060102"))
}

// CheckIfPartitionExists checks if a partition for the given date already exists.
func (m *PostgresDBManager) CheckIfPartitionExists(date time.Time) (bool, error) {
	tableName := getPartitionTableName(date)

	query := `
	SELECT 1
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relname = $1
	AND n.nspname = 'public'; -- Assuming the table is in the public schema

	`
	var exists int
	err := m.dbpool.QueryRow(m.ctx, query, tableName).Scan(&exists)

	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("error checking partition existence for %s: %w", tableName, err)
	}
	return true, nil
}

// CreatePartitionForDate creates a partition for a specific day if it doesn't exist.
// This function assumes the input date is normalized (e.g., midnight UTC).
func (m *PostgresDBManager) CreatePartitionForDate(date time.Time) error {
	tableName := getPartitionTableName(date)

	// Calculate range: [date_midnight, next_day_midnight)
	startRange := date.Format("2006-01-02 15:04:05")
	endRange := date.Add(24 * time.Hour).Format("2006-01-02 15:04:05")
	createQuery := fmt.Sprintf(`
	CREATE TABLE %s PARTITION OF trade_records
	FOR VALUES FROM ('%s') TO ('%s');
	`, pgx.Identifier{tableName}.Sanitize(), startRange, endRange)

	log.Printf("Creating partition %s for range [%s, %s)", tableName, startRange, endRange)

	_, err := m.dbpool.Exec(m.ctx, createQuery)
	if err != nil {
		if !m.isPartitionAlreadyExistsError(err) {
			return fmt.Errorf("error creating partition %s: %w", tableName, err)
		}
		log.Printf("Partition %s already exists, skipping creation.", tableName)
		return nil // Already exists, not an error
	}

	indexName := "idx_unique_" + tableName
	indexQuery := fmt.Sprintf(`
		CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (reference_date, hash);
	`, pgx.Identifier{indexName}.Sanitize(), pgx.Identifier{tableName}.Sanitize())

	log.Printf("Applying unique index %s to partition %s", indexName, tableName)
	_, err = m.dbpool.Exec(m.ctx, indexQuery)
	if err != nil {
		if !m.isPartitionAlreadyExistsError(err) {
			return fmt.Errorf("error applying unique index to partition %s: %w", tableName, err)
		}
		log.Printf("Unique index on partition %s already exists.", tableName)
	}

	return nil
}

func (m *PostgresDBManager) isPartitionAlreadyExistsError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "already exists") || strings.Contains(errStr, "duplicate key value violates unique constraint")
}

// InsertFileRecord inserts a new file record into the file_records table.
func (m *PostgresDBManager) InsertFileRecord(fileName string, date time.Time, status string, checksum string, referenceDate time.Time) (int, error) {
	query := `
	INSERT INTO file_records (file_name, processed_at, status, checksum, reference_date)
	VALUES ($1, $2, $3, $4, $5)
	RETURNING id;`

	var fileID int
	err := m.dbpool.QueryRow(m.ctx, query, fileName, date, status, checksum, referenceDate).Scan(&fileID)
	if err != nil {
		return 0, fmt.Errorf("error inserting file record: %v", err)
	}

	return fileID, nil
}

// UpdateFileChecksum updates the checksum of a file record in the database.
func (m *PostgresDBManager) UpdateFileChecksum(fileID int, checksum string) error {
	query := `
	UPDATE file_records
	SET checksum = $1
	WHERE id = $2;`

	_, err := m.dbpool.Exec(m.ctx, query, checksum, fileID)
	if err != nil {
		return fmt.Errorf("error updating file checksum: %v", err)
	}

	return nil
}

// UpdateFileStatus updates the status of a file record in the database.
func (m *PostgresDBManager) UpdateFileStatus(fileID int, status string, errors any) error {
	query := `
	UPDATE file_records
	SET status = $1,
		errors = $2
	WHERE id = $3;`

	_, err := m.dbpool.Exec(m.ctx, query, status, errors, fileID)
	if err != nil {
		return fmt.Errorf("error updating file status: %v", err)
	}

	return nil
}

// FindFileRecordByChecksum finds a file record by its checksum.
func (m *PostgresDBManager) FindFileRecordByChecksum(checksum string) (*models.FileRecord, error) {
	query := `
	SELECT id, file_name, processed_at, status, checksum, reference_date, errors
	FROM file_records
	WHERE checksum = $1;`

	fileRecord := &models.FileRecord{}
	var errors []byte // Use a byte slice for the JSON field

	err := m.dbpool.QueryRow(m.ctx, query, checksum).Scan(
		&fileRecord.ID,
		&fileRecord.FileName,
		&fileRecord.ProcessedAt,
		&fileRecord.Status,
		&fileRecord.Checksum,
		&fileRecord.ReferenceDate,
		&errors,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // Not found, but not an error
		}
		return nil, fmt.Errorf("error finding file record by checksum: %v", err)
	}

	// Only set errors if not null
	if errors != nil {
		fileRecord.Errors = errors
	}

	return fileRecord, nil
}

func (m *PostgresDBManager) CopyTradesIntoStagingTable(trades []*models.Trade, stagingTableName string) error {

	// Step 1: Bulk load into the worker's assigned staging table.
	log.Printf("Bulk loading %d trades into staging table %s", len(trades), stagingTableName)
	_, err := m.dbpool.CopyFrom(
		m.ctx,
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

	return nil
}

// InsertMultipleTrades inserts multiple trade records using the bulk load, filter, and insert pattern.
func (m *PostgresDBManager) InsertMultipleTrades(trades []*models.Trade, stagingTableName string) error {
	// Step 1: Bulk load into the worker's assigned staging table.
	log.Printf("Bulk loading %d trades into staging table %s", len(trades), stagingTableName)
	err := m.CopyTradesIntoStagingTable(trades, stagingTableName)
	if err != nil {
		return fmt.Errorf("unable to copy trades to staging table %s: %v", stagingTableName, err)
	}

	// Step 2: Insert by exclusion, reading from the worker's assigned staging table.
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
	_, err = m.dbpool.Exec(m.ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("error inserting from staging table %s: %v", stagingTableName, err)
	}

	// Step 3: Truncate the worker's staging table to prepare for its *next* batch.
	truncateQuery := fmt.Sprintf(`TRUNCATE %s;`, pgx.Identifier{stagingTableName}.Sanitize())
	log.Printf("Truncating staging table %s.", stagingTableName)
	_, err = m.dbpool.Exec(m.ctx, truncateQuery)
	if err != nil {
		// This is not a fatal error for the data itself, but should be logged as a warning.
		log.Printf("WARN: failed to truncate staging table %s: %v", stagingTableName, err)
	}

	return nil
}

// InsertAllStagingTableData inserts all data from a staging table into trade_records without checking for duplicates.
func (m *PostgresDBManager) InsertAllStagingTableData(trades []*models.Trade, stagingTableName string) error {
	// Step 1: Bulk load into the worker's assigned staging table.
	log.Printf("Bulk loading %d trades into staging table %s", len(trades), stagingTableName)
	err := m.CopyTradesIntoStagingTable(trades, stagingTableName)
	if err != nil {
		return fmt.Errorf("unable to copy trades to staging table %s: %v", stagingTableName, err)
	}

	// Step 2: Insert all data from the staging table to the main table
	insertQuery := fmt.Sprintf(`
	INSERT INTO trade_records (hash, reference_date, transaction_date, ticker, identifier, price, quantity, closing_time, file_id)
	SELECT hash, reference_date, transaction_date, ticker, identifier, price, quantity, closing_time, file_id
	FROM %s;
	`, pgx.Identifier{stagingTableName}.Sanitize())

	log.Printf("Inserting all data from staging table %s to main table.", stagingTableName)
	_, err = m.dbpool.Exec(m.ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("error inserting all data from staging table %s: %v", stagingTableName, err)
	}

	return nil
}

// InsertDiffFromStagingTable inserts the difference between staging table data and trade_records
// using a CTE to identify records in the staging table that are not in trade_records by hash value.
func (m *PostgresDBManager) InsertDiffFromStagingTable(trades []*models.Trade, stagingTableName string) error {
	log.Printf("Bulk loading %d trades into staging table %s", len(trades), stagingTableName)
	err := m.CopyTradesIntoStagingTable(trades, stagingTableName)
	if err != nil {
		return fmt.Errorf("unable to copy trades to staging table %s: %v", stagingTableName, err)
	}
	insertDiffQuery := fmt.Sprintf(`
	WITH staging_diff AS (
		SELECT s.hash, s.reference_date, s.transaction_date, s.ticker, s.identifier, s.price, s.quantity, s.closing_time, s.file_id
		FROM %s s
		LEFT JOIN trade_records t ON t.hash = s.hash AND t.reference_date = s.reference_date
		WHERE t.hash IS NULL
	)
	INSERT INTO trade_records (hash, reference_date, transaction_date, ticker, identifier, price, quantity, closing_time, file_id)
	SELECT hash, reference_date, transaction_date, ticker, identifier, price, quantity, closing_time, file_id
	FROM staging_diff;
	`, pgx.Identifier{stagingTableName}.Sanitize())

	log.Printf("Inserting differences from staging table %s to main table using CTE.", stagingTableName)
	_, err = m.dbpool.Exec(m.ctx, insertDiffQuery)
	if err != nil {
		return fmt.Errorf("error inserting differences from staging table %s: %v", stagingTableName, err)
	}

	return nil
}
