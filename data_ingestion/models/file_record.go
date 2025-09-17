package models

import (
	"encoding/json"
	"time"
)

// FileRecord represents a record in the file_records table
type FileRecord struct {
	ID           int             `json:"id"`
	FileName     string          `json:"file_name"`
	ProcessedAt  time.Time       `json:"processed_at"`
	Status       string          `json:"status"`
	Checksum     string          `json:"checksum"`
	ReferenceDate time.Time      `json:"reference_date"`
	Errors       json.RawMessage `json:"errors"`
}
