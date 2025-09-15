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

	// Get max_range_value
	queryMaxPrice := `
		SELECT COALESCE(MAX(preco_negocio), 0)
		FROM trade_records
		WHERE codigo_instrumento = $1 AND data_negocio >= $2;`

	err := m.Pool.QueryRow(ctx, queryMaxPrice, ticker, startDate).Scan(&info.MaxRangeValue)
	if err != nil {
		return nil, fmt.Errorf("error querying max price: %w", err)
	}

	// Get max_daily_volume
	queryMaxVolume := `
		WITH daily_volumes AS (
			SELECT
				DATE(data_negocio) AS trade_day,
				SUM(quantidade_negociada) AS total_volume
			FROM trade_records
			WHERE codigo_instrumento = $1 AND data_negocio >= $2
			GROUP BY trade_day
		)
		SELECT COALESCE(MAX(total_volume), 0) FROM daily_volumes;`

	err = m.Pool.QueryRow(ctx, queryMaxVolume, ticker, startDate).Scan(&info.MaxDailyVolume)
	if err != nil {
		return nil, fmt.Errorf("error querying max daily volume: %w", err)
	}

	return info, nil
}

func (m *PostgresTickerDBManager) Close() error {
	if m.Pool != nil {
		m.Pool.Close()
	}
	return nil
}
