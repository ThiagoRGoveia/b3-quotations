package models

import "time"

type Trade struct {
	TransactionDate time.Time `json:"transaction_date,omitempty"`
	ReferenceDate   time.Time `json:"reference_date,omitempty"`
	Ticker          string    `json:"ticker,omitempty"`
	Price           float64   `json:"price,omitempty"`
	Quantity        int64     `json:"quantity,omitempty"`
	ClosingTime     string    `json:"closing_time,omitempty"`
	Identifier      string    `json:"identifier,omitempty"`
	FileID          int       `json:"file_id,omitempty"`
	Hash            string    `json:"hash,omitempty"`
}

func (t *Trade) IsValid() bool {
	return t.ReferenceDate != time.Time{} &&
		t.TransactionDate != time.Time{} &&
		t.Ticker != "" &&
		t.ClosingTime != ""
}
