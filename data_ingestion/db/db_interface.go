package db

import (
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/models"
)

// DBManager defines the interface for database operations.
type DBManager interface {
	CreateFileRecordsTable() error
	CreateTradeRecordsTable() error
	CreateTradeRecordIndexes() error
	DropTradeRecordIndexes() error
	InsertFileRecord(fileName string, date time.Time, status string) (int, error)
	InsertMultipleTrades(trades []*models.Trade) error
	UpdateFileStatus(fileID int, status string, errors []string) error
}
