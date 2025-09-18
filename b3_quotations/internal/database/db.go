package database

import (
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
)

type DBManager interface {
	CreateFileRecordsTable() error
	CreateTradeRecordsTable() error
	CreateTradeRecordIndexes() error
	DropTradeRecordIndexes() error
	InsertFileRecord(fileName string, date time.Time, status string, checksum string, referenceDate time.Time) (int, error)
	UpdateFileStatus(fileID int, status string, errors any) error
	CreateWorkerStagingTables(numTables int) ([]string, error)
	DropWorkerStagingTable(tableName string) error
	CreatePartitionsForDates(dates []time.Time) (*models.FirstWritePartition, error)
	InsertDiffFromStagingTable(trades []*models.Trade, stagingTableName string) error
	InsertAllStagingTableData(trades []*models.Trade, stagingTableName string) error
	IsFileAlreadyProcessed(checksum string) (bool, error)
}
