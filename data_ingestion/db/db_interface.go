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
	InsertFileRecord(fileName string, date time.Time, status string, checksum string, referenceDate time.Time) (int, error)
	UpdateFileChecksum(fileID int, checksum string) error
	UpdateFileStatus(fileID int, status string, errors any) error
	CreateWorkerStagingTable(tableName string) error
	CreateWorkerStagingTables(numTables int) ([]string, error)
	DropWorkerStagingTable(tableName string) error
	CheckIfPartitionExists(date time.Time) (bool, error)
	CreatePartitionForDate(date time.Time) error
	CreatePartitionsForDates(dates []time.Time) (*FirstWritePartition, error)
	InsertDiffFromStagingTable(trades []*models.Trade, stagingTableName string) error
	InsertAllStagingTableData(trades []*models.Trade, stagingTableName string) error
	IsFileAlreadyProcessed(checksum string) (bool, error)
}
