package db

import (
	"context"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/models"
)

// DBManager defines the interface for database operations.
type DBManager interface {
	CreateFileRecordsTable() error
	CreateTradeRecordsTable() error
	DropTradeRecordIndexes() error
	InsertFileRecord(fileName string, date time.Time, status string) (int, error)
	UpdateFileStatus(fileID int, status string, errors any) error
	CreateWorkerStagingTable(tableName string) error
	DropWorkerStagingTable(tableName string) error
	InsertMultipleTrades(ctx context.Context, trades []*models.Trade, stagingTableName string) error
	CheckIfPartitionExists(ctx context.Context, date time.Time) (bool, error)
	CreatePartitionForDate(ctx context.Context, date time.Time) error
}
