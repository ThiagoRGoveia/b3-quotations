package database

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ConnectDB(connStr string) (*pgxpool.Pool, error) {
	dbpool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %v", err)
	}

	return dbpool, nil
}

type PostgresDBManager struct {
	dbpool *pgxpool.Pool
	ctx    context.Context
}

func NewPostgresDBManager(ctx context.Context, pool *pgxpool.Pool) *PostgresDBManager {
	return &PostgresDBManager{dbpool: pool, ctx: ctx}
}

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

// CreateTradeRecordsTable creates the trade_records partition table. The partitions are split daily and will
// hold approx 8M records each
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
		checksum VARCHAR(64) NOT NULL
	) PARTITION BY RANGE (reference_date); 
	`
	_, err := m.dbpool.Exec(m.ctx, query)
	if err != nil {
		return fmt.Errorf("error creating trade_records partition table: %v", err)
	}

	return nil
}

func (m *PostgresDBManager) CreateWorkerStagingTables(numTables int) ([]string, error) {
	if numTables <= 0 {
		return nil, nil
	}

	stagingTableNames := make([]string, numTables)
	for w := 1; w <= numTables; w++ {
		stagingTableNames[w-1] = fmt.Sprintf("trade_records_staging_worker_%d", w)
	}

	tx, err := m.dbpool.Begin(m.ctx)
	if err != nil {
		return nil, fmt.Errorf("error beginning transaction: %v", err)
	}

	existingTables := make(map[string]bool)
	placeholders := make([]string, len(stagingTableNames))
	args := make([]interface{}, len(stagingTableNames))

	for i, name := range stagingTableNames {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = name
	}

	checkQuery := fmt.Sprintf(
		`SELECT tablename FROM pg_tables WHERE tablename = ANY(ARRAY[%s])`,
		strings.Join(placeholders, ", "))

	rows, err := tx.Query(m.ctx, checkQuery, args...)
	if err != nil {
		rx := tx.Rollback(m.ctx)
		if rx != nil {
			log.Printf("Error rolling back transaction: %v", rx)
		}
		return nil, fmt.Errorf("error checking existing staging tables: %w", err)
	}

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			rows.Close()
			rx := tx.Rollback(m.ctx)
			if rx != nil {
				log.Printf("Error rolling back transaction: %v", rx)
			}
			return nil, fmt.Errorf("error scanning tablename: %w", err)
		}
		existingTables[tableName] = true
	}

	rows.Close()
	if err := rows.Err(); err != nil {
		rx := tx.Rollback(m.ctx)
		if rx != nil {
			log.Printf("Error rolling back transaction: %v", rx)
		}
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	for _, tableName := range stagingTableNames {
		if !existingTables[tableName] {
			query := fmt.Sprintf(`CREATE UNLOGGED TABLE IF NOT EXISTS %s (LIKE trade_records INCLUDING DEFAULTS);`,
				pgx.Identifier{tableName}.Sanitize())

			_, err := tx.Exec(m.ctx, query)
			if err != nil {
				rx := tx.Rollback(m.ctx)
				if rx != nil {
					log.Printf("Error rolling back transaction: %v", rx)
				}
				return nil, fmt.Errorf("error creating worker staging table %s: %v", tableName, err)
			}
			log.Printf("Created staging table %s", tableName)
		} else {
			log.Printf("Staging table %s already exists, skipping creation", tableName)
		}
	}

	if err := tx.Commit(m.ctx); err != nil {
		return nil, fmt.Errorf("error committing transaction: %v", err)
	}

	return stagingTableNames, nil
}

func (m *PostgresDBManager) DropWorkerStagingTable(tableName string) error {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, pgx.Identifier{tableName}.Sanitize())
	_, err := m.dbpool.Exec(m.ctx, query)
	if err != nil {
		return fmt.Errorf("error dropping worker staging table %s: %v", tableName, err)
	}
	return nil
}

func (m *PostgresDBManager) CreateTradeRecordIndexes() error {
	queries := []string{
		`CREATE INDEX IF NOT EXISTS idx_trade_records_optimized ON trade_records (ticker, transaction_date) INCLUDE (price, quantity, reference_date);`,
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
		`DROP INDEX IF EXISTS idx_trade_records_optimized`,
	}

	for _, query := range queries {
		_, err := m.dbpool.Exec(m.ctx, query)
		if err != nil {
			return fmt.Errorf("error dropping index: %v", err)
		}
	}

	return nil
}

func getPartitionTableName(date time.Time) string {
	return fmt.Sprintf("trade_records_%s", date.Format("20060102"))
}

func (m *PostgresDBManager) isPartitionAlreadyExistsError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "already exists") || strings.Contains(errStr, "duplicate key value violates unique constraint")
}

func (m *PostgresDBManager) CreatePartitionsForDates(dates []time.Time) (*models.FirstWritePartition, error) {
	if len(dates) == 0 {
		return nil, nil
	}

	tableNames := make([]string, 0, len(dates))
	dateToTableName := make(map[time.Time]string, len(dates))
	createdPartitions := make(models.FirstWritePartition)

	for _, date := range dates {
		tableName := getPartitionTableName(date)
		tableNames = append(tableNames, tableName)
		dateToTableName[date] = tableName
		createdPartitions[date] = true
	}

	tx, err := m.dbpool.Begin(m.ctx)
	if err != nil {
		return nil, fmt.Errorf("error beginning transaction: %v", err)
	}

	existingTables := make(map[string]bool)
	placeholders := make([]string, len(tableNames))
	args := make([]interface{}, len(tableNames))

	for i, name := range tableNames {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = name
	}

	checkQuery := fmt.Sprintf(
		`SELECT tablename FROM pg_tables WHERE tablename = ANY(ARRAY[%s])`,
		strings.Join(placeholders, ", "))

	rows, err := tx.Query(m.ctx, checkQuery, args...)
	if err != nil {
		rx := tx.Rollback(m.ctx)
		if rx != nil {
			log.Printf("Error rolling back transaction: %v", rx)
		}
		return nil, fmt.Errorf("error checking existing partitions: %w", err)
	}

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			rows.Close()
			rx := tx.Rollback(m.ctx)
			if rx != nil {
				log.Printf("Error rolling back transaction: %v", rx)
			}
			return nil, fmt.Errorf("error scanning tablename: %w", err)
		}
		existingTables[tableName] = true
	}

	rows.Close()
	if err := rows.Err(); err != nil {
		rx := tx.Rollback(m.ctx)
		if rx != nil {
			log.Printf("Error rolling back transaction: %v", rx)
		}
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	for _, date := range dates {
		tableName := dateToTableName[date]

		if !existingTables[tableName] {
			startRange := date.Format("2006-01-02 15:04:05")
			endRange := date.Add(24 * time.Hour).Format("2006-01-02 15:04:05")
			createQuery := fmt.Sprintf(
				`CREATE TABLE %s PARTITION OF trade_records
				FOR VALUES FROM ('%s') TO ('%s');`,
				pgx.Identifier{tableName}.Sanitize(), startRange, endRange)

			log.Printf("Creating partition %s for range [%s, %s)", tableName, startRange, endRange)

			_, err := tx.Exec(m.ctx, createQuery)
			if err != nil {
				if !m.isPartitionAlreadyExistsError(err) {
					rx := tx.Rollback(m.ctx)
					if rx != nil {
						log.Printf("Error rolling back transaction: %v", rx)
					}
					return nil, fmt.Errorf("error creating partition %s: %w", tableName, err)
				}
				log.Printf("Partition %s already exists, skipping creation.", tableName)
			}
		} else {
			// if the partition already exists, it is not the first write
			createdPartitions[date] = false
			log.Printf("Partition for date %s already exists. Skipping creation.", date.Format("2006-01-02"))
		}
	}

	if err := tx.Commit(m.ctx); err != nil {
		return nil, fmt.Errorf("error committing transaction: %v", err)
	}

	return &createdPartitions, nil
}

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

func (m *PostgresDBManager) IsFileAlreadyProcessed(checksum string) (bool, error) {
	query := `
	SELECT id
	FROM file_records
	WHERE checksum = $1 AND status = 'DONE';`

	var id int

	err := m.dbpool.QueryRow(m.ctx, query, checksum).Scan(&id)

	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("error finding file record by checksum: %v", err)
	}

	return true, nil
}

func (m *PostgresDBManager) CopyTradesIntoStagingTable(tx pgx.Tx, trades []*models.Trade, stagingTableName string) error {
	// The column order here must match the order in the `trade_records` table.
	columnNames := []string{
		"checksum", "reference_date", "transaction_date", "ticker", "identifier", "price", "quantity", "closing_time", "file_id",
	}

	copySource := pgx.CopyFromSlice(len(trades), func(i int) ([]interface{}, error) {
		trade := trades[i]
		return []interface{}{trade.CheckSum, trade.ReferenceDate, trade.TransactionDate, trade.Ticker, trade.Identifier, trade.Price, trade.Quantity, trade.ClosingTime, trade.FileID},
			nil
	})

	_, err := tx.CopyFrom(
		m.ctx,
		pgx.Identifier{stagingTableName},
		columnNames,
		copySource,
	)

	return err
}

func (m *PostgresDBManager) InsertAllStagingTableData(trades []*models.Trade, stagingTableName string) error {
	tx, err := m.dbpool.Begin(m.ctx)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %v", err)
	}
	defer tx.Rollback(m.ctx)

	log.Printf("Bulk loading %d trades into staging table %s", len(trades), stagingTableName)
	err = m.CopyTradesIntoStagingTable(tx, trades, stagingTableName)
	if err != nil {
		return fmt.Errorf("unable to copy trades to staging table %s: %v", stagingTableName, err)
	}

	insertQuery := fmt.Sprintf(`
	INSERT INTO trade_records (checksum, reference_date, transaction_date, ticker, identifier, price, quantity, closing_time, file_id)
	SELECT checksum, reference_date, transaction_date, ticker, identifier, price, quantity, closing_time, file_id
	FROM %s;
	`, pgx.Identifier{stagingTableName}.Sanitize())

	log.Printf("Inserting all data from staging table %s to main table.", stagingTableName)
	_, err = tx.Exec(m.ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("error inserting all data from staging table %s: %v", stagingTableName, err)
	}

	truncateQuery := fmt.Sprintf(`TRUNCATE %s;`, pgx.Identifier{stagingTableName}.Sanitize())
	log.Printf("Truncating staging table %s.", stagingTableName)
	_, err = tx.Exec(m.ctx, truncateQuery)
	if err != nil {
		log.Printf("WARN: failed to truncate staging table %s: %v", stagingTableName, err)
	}

	return tx.Commit(m.ctx)
}

// InsertDiffFromStagingTable inserts the difference between staging table data and trade_records
// using a CTE to identify records in the staging table that are not in trade_records by checksum value.
func (m *PostgresDBManager) InsertDiffFromStagingTable(trades []*models.Trade, stagingTableName string) error {
	tx, err := m.dbpool.Begin(m.ctx)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %v", err)
	}
	defer tx.Rollback(m.ctx)

	log.Printf("Bulk loading %d trades into staging table %s", len(trades), stagingTableName)
	err = m.CopyTradesIntoStagingTable(tx, trades, stagingTableName)
	if err != nil {
		return fmt.Errorf("unable to copy trades to staging table %s: %v", stagingTableName, err)
	}
	insertDiffQuery := fmt.Sprintf(`
	WITH staging_diff AS (
		SELECT s.checksum, s.reference_date, s.transaction_date, s.ticker, s.identifier, s.price, s.quantity, s.closing_time, s.file_id
		FROM %s s
		WHERE NOT EXISTS (
			SELECT 1
			FROM trade_records t
			WHERE t.checksum = s.checksum AND t.reference_date = s.reference_date
		)
	)
	INSERT INTO trade_records (checksum, reference_date, transaction_date, ticker, identifier, price, quantity, closing_time, file_id)
	SELECT checksum, reference_date, transaction_date, ticker, identifier, price, quantity, closing_time, file_id
	FROM staging_diff;
	`, pgx.Identifier{stagingTableName}.Sanitize())

	log.Printf("Inserting differences from staging table %s to main table using CTE.", stagingTableName)
	_, err = tx.Exec(m.ctx, insertDiffQuery)
	if err != nil {
		return fmt.Errorf("error inserting differences from staging table %s: %v", stagingTableName, err)
	}

	truncateQuery := fmt.Sprintf(`TRUNCATE %s;`, pgx.Identifier{stagingTableName}.Sanitize())
	log.Printf("Truncating staging table %s.", stagingTableName)
	_, err = tx.Exec(m.ctx, truncateQuery)
	if err != nil {
		log.Printf("WARN: failed to truncate staging table %s: %v", stagingTableName, err)
	}

	return tx.Commit(m.ctx)
}

func (m *PostgresDBManager) GetTickerInfo(ticker string, startDate time.Time) (*models.TickerInfo, error) {
	info := &models.TickerInfo{Ticker: ticker}

	query := `
		WITH filtered_trades AS (
    SELECT
        price,
        quantity,
        transaction_date
    FROM
        trade_records
    WHERE
        ticker = $1 AND reference_date >= $2 AND transaction_date >= $2
		),
		daily_volumes AS (
				SELECT
						DATE(transaction_date) AS trade_date,
						SUM(quantity) AS total_volume
				FROM
						filtered_trades
				GROUP BY
						trade_date
		)
		SELECT
				(SELECT COALESCE(MAX(price), 0) FROM filtered_trades) AS max_range_value,
				(SELECT COALESCE(MAX(total_volume), 0) FROM daily_volumes) AS max_daily_volume;`

	err := m.dbpool.QueryRow(m.ctx, query, ticker, startDate).Scan(&info.MaxRangeValue, &info.MaxDailyVolume)
	if err != nil {
		return nil, fmt.Errorf("error querying ticker info: %w", err)
	}

	return info, nil
}
