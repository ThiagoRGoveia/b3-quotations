package db

import (
	"context"
	"fmt"
	"time"

	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CreateFileRecordTable creates the file_records table in the database.
func CreateFileRecordTable(dbpool *pgxpool.Pool) error {
	query := `
	CREATE TABLE IF NOT EXISTS file_records (
		id SERIAL PRIMARY KEY,
		file_name VARCHAR(255) NOT NULL,
		date TIMESTAMP NOT NULL,
		status VARCHAR(50) NOT NULL CHECK (status IN ('DONE', 'PROCESSING', 'ERROR'))
	);`

	_, err := dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error creating file_records table: %v", err)
	}

	return nil
}

// CreateTradeLoadRecordTable creates the trade_loaded_records table in the database.
func CreateTradeLoadRecordTable(dbpool *pgxpool.Pool) error {
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

	_, err := dbpool.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error creating trade_loaded_records table: %v", err)
	}

	return nil
}

// InsertFileRecord inserts a new file record into the file_records table.
func InsertFileRecord(dbpool *pgxpool.Pool, fileName string, date time.Time, status string) (int, error) {
	query := `
	INSERT INTO file_records (file_name, date, status)
	VALUES ($1, $2, $3)
	RETURNING id;`

	var fileID int
	err := dbpool.QueryRow(context.Background(), query, fileName, date, status).Scan(&fileID)
	if err != nil {
		return 0, fmt.Errorf("error inserting file record: %v", err)
	}

	return fileID, nil
}

// InsertTrade inserts a new trade record into the trade_loaded_records table.
func InsertTrade(dbpool *pgxpool.Pool, trade *models.Trade, isValid bool) (int, error) {
	query := `
	INSERT INTO trade_loaded_records (data_negocio, codigo_instrumento, preco_negocio, quantidade_negociada, hora_fechamento, is_valid, file_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	RETURNING id;`

	var tradeID int
	err := dbpool.QueryRow(context.Background(), query, trade.DataNegocio, trade.CodigoInstrumento, trade.PrecoNegocio, trade.QuantidadeNegociada, trade.HoraFechamento, isValid, trade.FileID).Scan(&tradeID)
	if err != nil {
		return 0, fmt.Errorf("error inserting trade: %v", err)
	}

	return tradeID, nil
}

// UpdateFileStatus updates the status of a file record in the database.
func UpdateFileStatus(dbpool *pgxpool.Pool, fileID int, status string) error {
	query := `
	UPDATE file_records
	SET status = $1
	WHERE id = $2;`

	_, err := dbpool.Exec(context.Background(), query, status, fileID)
	if err != nil {
		return fmt.Errorf("error updating file status: %v", err)
	}

	return nil
}
