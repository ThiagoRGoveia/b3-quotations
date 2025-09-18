package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	DatabaseURL                  string
	NumParserWorkers             int
	NumDBWorkersPerReferenceDate int
	ResultsChannelSize           int
	DBBatchSize                  int
}

func New() (*Config, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable is not set")
	}

	cfg := &Config{
		DatabaseURL:                  databaseURL,
		NumParserWorkers:             7,
		NumDBWorkersPerReferenceDate: 2,
		ResultsChannelSize:           500000,
		DBBatchSize:                  80000,
	}

	var err error
	cfg.NumParserWorkers, err = getEnvAsInt("NUM_PARSER_WORKERS", cfg.NumParserWorkers)
	if err != nil {
		return nil, err
	}

	cfg.NumDBWorkersPerReferenceDate, err = getEnvAsInt("NUM_DB_WORKERS_PER_DATE", cfg.NumDBWorkersPerReferenceDate)
	if err != nil {
		return nil, err
	}

	cfg.ResultsChannelSize, err = getEnvAsInt("RESULTS_CHANNEL_SIZE", cfg.ResultsChannelSize)
	if err != nil {
		return nil, err
	}

	cfg.DBBatchSize, err = getEnvAsInt("DB_BATCH_SIZE", cfg.DBBatchSize)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func getEnvAsInt(key string, defaultValue int) (int, error) {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return 0, fmt.Errorf("invalid value for %s: expected an integer, got '%s'", key, valueStr)
	}

	return value, nil
}
