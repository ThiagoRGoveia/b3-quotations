package db

import (
	"time"

	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/models"
)

// DBManager defines the interface for database operations.
type DBManager interface {
	CreateFileRecordTable() error
	CreateTradeLoadedRecordTable() error
	CreateTradeRecordsTable() error
	InsertFileRecord(fileName string, date time.Time, status string) (int, error)
	InsertTrade(trade *models.Trade, isValid bool) (int, error)
	InsertMultipleTrades(trades []*models.Trade, isValid bool) error
	UpdateFileStatus(fileID int, status string, errors []string) error
	ValidateSavedData(fileIDs []int) error
	TransferDataToFinalTable(fileIDs []int) error
	CleanTempData(fileIDs []int) error
}
