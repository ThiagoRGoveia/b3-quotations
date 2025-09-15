package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/models"
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
func (m *PostgresDBManager) CreateFileRecordTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS file_records (
		id SERIAL PRIMARY KEY,
		file_name VARCHAR(255) NOT NULL,
		date TIMESTAMP NOT NULL,
		status VARCHAR(50) NOT NULL CHECK (status IN ('DONE', 'DONE_WITH_ERRORS', 'PROCESSING', 'FATAL')),
		errors jsonb
	);`

	_, err := m.dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error creating file_records table: %v", err)
	}

	return nil
}

// CreateTradeLoadRecordTable creates the trade_loaded_records table in the database.
func (m *PostgresDBManager) CreateTradeLoadedRecordTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS trade_loaded_records (
		id SERIAL PRIMARY KEY,
		data_negocio TIMESTAMP,
		codigo_instrumento VARCHAR(255),
		preco_negocio NUMERIC(18, 2),
		quantidade_negociada BIGINT,
		hora_fechamento VARCHAR(50),
		is_valid BOOLEAN,
		file_id INTEGER,
		FOREIGN KEY (file_id) REFERENCES file_records(id)
	);`

	_, err := m.dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error creating trade_loaded_records table: %v", err)
	}

	return nil
}

func (m *PostgresDBManager) CreateTradeRecordsTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS trade_records (
		id SERIAL PRIMARY KEY,
		data_negocio TIMESTAMP NOT NULL,
		codigo_instrumento VARCHAR(255) NOT NULL,
		preco_negocio NUMERIC(18, 2) NOT NULL,
		quantidade_negociada BIGINT NOT NULL,
		hora_fechamento VARCHAR(50) NOT NULL
	);`

	_, err := m.dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error creating trade_records table: %v", err)
	}

	return nil
}

// InsertFileRecord inserts a new file record into the file_records table.
func (m *PostgresDBManager) InsertFileRecord(fileName string, date time.Time, status string) (int, error) {
	query := `
	INSERT INTO file_records (file_name, date, status)
	VALUES ($1, $2, $3)
	RETURNING id;`

	var fileID int
	err := m.dbpool.QueryRow(context.Background(), query, fileName, date, status).Scan(&fileID)
	if err != nil {
		return 0, fmt.Errorf("error inserting file record: %v", err)
	}

	return fileID, nil
}

// InsertTrade inserts a new trade record into the trade_loaded_records table.
func (m *PostgresDBManager) InsertTrade(trade *models.Trade, isValid bool) (int, error) {
	query := `
	INSERT INTO trade_loaded_records (data_negocio, codigo_instrumento, preco_negocio, quantidade_negociada, hora_fechamento, is_valid, file_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	RETURNING id;`

	var tradeID int
	err := m.dbpool.QueryRow(context.Background(), query, trade.DataNegocio, trade.CodigoInstrumento, trade.PrecoNegocio, trade.QuantidadeNegociada, trade.HoraFechamento, isValid, trade.FileID).Scan(&tradeID)
	if err != nil {
		return 0, fmt.Errorf("error inserting trade: %v", err)
	}

	return tradeID, nil
}

// UpdateFileStatus updates the status of a file record in the database.
func (m *PostgresDBManager) UpdateFileStatus(fileID int, status string, errors []string) error {
	query := `
	UPDATE file_records
	SET status = $1,
		errors = $2,
	WHERE id = $3;`

	_, err := m.dbpool.Exec(context.Background(), query, status, errors, fileID)
	if err != nil {
		return fmt.Errorf("error updating file status: %v", err)
	}

	return nil
}

func (m *PostgresDBManager) ValidateSavedData(fileID int) {
	query := `
	UPDATE trade_loaded_records
	SET is_valid = true
	WHERE file_id = $1 AND
		is_valid = false
		data_negocio IS NOT NULL AND 
		codigo_instrumento IS NOT NULL AND 
		preco_negocio IS NOT NULL AND 
		quantidade_negociada IS NOT NULL AND 
		hora_fechamento IS NOT NULL;`

	_, err := m.dbpool.Exec(context.Background(), query, fileID)
	if err != nil {
		log.Printf("Failed to validate saved data for fileID %d: %v\n", fileID, err)
	}
}

func (m *PostgresDBManager) TransferDataToFinalTable(fileID int) {
	query := `
	INSERT INTO trade_records
	SELECT * FROM trade_loaded_records
	WHERE file_id = $1 AND is_valid = true;`

	_, err := m.dbpool.Exec(context.Background(), query, fileID)
	if err != nil {
		log.Printf("Failed to transfer data to final table for fileID %d: %v\n", fileID, err)
	}
}

// InsertMultipleTrades inserts multiple trade records in a single transaction.
func (m *PostgresDBManager) InsertMultipleTrades(trades []*models.Trade, isValid bool) error {
	_, err := m.dbpool.CopyFrom(
		context.Background(),
		pgx.Identifier{"trade_loaded_records"},
		[]string{"data_negocio", "codigo_instrumento", "preco_negocio", "quantidade_negociada", "hora_fechamento", "is_valid", "file_id"},
		pgx.CopyFromSlice(len(trades), func(i int) ([]any, error) {
			trade := trades[i]
			return []any{trade.DataNegocio, trade.CodigoInstrumento, trade.PrecoNegocio, trade.QuantidadeNegociada, trade.HoraFechamento, isValid, trade.FileID}, nil
		}),
	)

	if err != nil {
		return fmt.Errorf("unable to copy trades from slice: %v", err)
	}

	return nil
}
