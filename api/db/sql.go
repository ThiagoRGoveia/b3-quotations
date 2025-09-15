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
		WITH
			max_price_calc AS (
				SELECT
					COALESCE(MAX(preco_negocio), 0) AS max_range_value
				FROM
					trade_records
				WHERE
					codigo_instrumento = $1 AND data_negocio >= $2
			),
			daily_volumes AS (
				SELECT
					SUM(quantidade_negociada) AS total_volume
				FROM
					trade_records
				WHERE
					codigo_instrumento = $1 AND data_negocio >= $2
				GROUP BY
					DATE(data_negocio)
			),
			max_volume_calc AS (
				SELECT
					COALESCE(MAX(total_volume), 0) AS max_daily_volume
				FROM
					daily_volumes
			)
		SELECT
			max_range_value,
			max_daily_volume
		FROM
			max_price_calc,
			max_volume_calc;`

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
