package db

import (
	"context"
	"time"
)

type TickerDBManager interface {
	GetTickerInfo(ctx context.Context, ticker string, startDate time.Time) (*TickerInfo, error)
	Close() error
}

type TickerInfo struct {
	Ticker         string  `json:"ticker"`
	MaxRangeValue  float64 `json:"max_range_value"`
	MaxDailyVolume int64   `json:"max_daily_volume"`
}
