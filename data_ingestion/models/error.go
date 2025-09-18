package models

import (
	"encoding/json"
	"fmt"
)

type AppError struct {
	FileID  int
	Message string
	Err     error
	Trade   *Trade
}

func (e *AppError) Error() string {
	var tradeDetails string
	if e.Trade != nil {
		tradeJSON, err := json.Marshal(e.Trade)
		if err != nil {
			tradeDetails = "failed to marshal trade to JSON"
		} else {
			tradeDetails = string(tradeJSON)
		}
	}

	if e.Err != nil {
		if tradeDetails != "" {
			return fmt.Sprintf("FileID %d: %s - %v - Trade: %s", e.FileID, e.Message, e.Err, tradeDetails)
		}
		return fmt.Sprintf("FileID %d: %s - %v", e.FileID, e.Message, e.Err)
	}

	if tradeDetails != "" {
		return fmt.Sprintf("FileID %d: %s - Trade: %s", e.FileID, e.Message, tradeDetails)
	}

	return fmt.Sprintf("FileID %d: %s", e.FileID, e.Message)
}
