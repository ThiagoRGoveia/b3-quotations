package db

import (
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/models"
)

// DBManager defines the interface for database operations.
type DBManager interface {
	CreateFileRecordsTable() error
	CreateTradeRecordsTable() error
	DropTradeRecordIndexes() error
	InsertFileRecord(fileName string, date time.Time, status string, checksum string, referenceDate time.Time) (int, error)
	UpdateFileChecksum(fileID int, checksum string) error
	UpdateFileStatus(fileID int, status string, errors any) error
	CreateWorkerStagingTable(tableName string) error
	CreateWorkerStagingTables(numTables int) ([]string, error)
	DropWorkerStagingTable(tableName string) error
	InsertMultipleTrades(trades []*models.Trade, stagingTableName string) error
	CheckIfPartitionExists(date time.Time) (bool, error)
	CreatePartitionForDate(date time.Time) error
	CreatePartitionsForDates(dates []time.Time) error
	InsertDiffFromStagingTable(trades []*models.Trade, stagingTableName string) error
	InsertAllStagingTableData(trades []*models.Trade, stagingTableName string) error
	CopyTradesIntoStagingTable(trades []*models.Trade, stagingTableName string) error
	FindFileRecordByChecksum(checksum string) (*models.FileRecord, error)
}
