package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresTickerDBManager struct {
	Pool *pgxpool.Pool
}

func NewPostgresTickerDBManager(ctx context.Context, connStr string) (*PostgresTickerDBManager, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	log.Println("Database connection established")
	return &PostgresTickerDBManager{Pool: pool}, nil
}

func (m *PostgresTickerDBManager) GetTickerInfo(ctx context.Context, ticker string, startDate time.Time) (*TickerInfo, error) {
	info := &TickerInfo{Ticker: ticker}

	query := `
		WITH filtered_trades AS (
    SELECT
        price,
        quantity,
        transaction_date
    FROM
        trade_records
    WHERE
        ticker = $1 AND transaction_date >= $2
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

	err := m.Pool.QueryRow(ctx, query, ticker, startDate).Scan(&info.MaxRangeValue, &info.MaxDailyVolume)
	if err != nil {
		return nil, fmt.Errorf("error querying ticker info: %w", err)
	}

	return info, nil
}

func (m *PostgresTickerDBManager) Close() error {
	if m.Pool != nil {
		m.Pool.Close()
	}
	return nil
}
