package db

import (
	"time"

	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DBManager defines the interface for database operations.
type DBManager interface {
	InsertFileRecord(dbpool *pgxpool.Pool, fileName string, date time.Time, status string) (int, error)
	InsertTrade(dbpool *pgxpool.Pool, trade *models.Trade, isValid bool) (int, error)
	UpdateFileStatus(dbpool *pgxpool.Pool, fileID int, status string) error
}
