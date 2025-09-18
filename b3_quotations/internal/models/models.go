package models

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Trade struct {
	TransactionDate time.Time `json:"transaction_date,omitempty"`
	ReferenceDate   time.Time `json:"reference_date,omitempty"`
	Ticker          string    `json:"ticker,omitempty"`
	Price           float64   `json:"price,omitempty"`
	Quantity        int64     `json:"quantity,omitempty"`
	ClosingTime     string    `json:"closing_time,omitempty"`
	Identifier      string    `json:"identifier,omitempty"`
	FileID          int       `json:"file_id,omitempty"`
	CheckSum        string    `json:"checksum,omitempty"`
}

func (t *Trade) IsValid() bool {
	return t.ReferenceDate != time.Time{} &&
		t.TransactionDate != time.Time{} &&
		t.Ticker != "" &&
		t.ClosingTime != ""
}

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

type FirstWritePartition map[time.Time]bool

type FileProcessingJob struct {
	FilePath string
	FileID   int
}

type FileInfo struct {
	Path          string
	ReferenceDate time.Time
}

type FileErrorMap struct {
	Errors map[int][]AppError
	Mu     sync.Mutex
}

type ExtractionChannels struct {
	Results map[time.Time]chan *Trade
	Errors  chan AppError
	Jobs    chan FileProcessingJob
}

type ExtractionWaitGroups struct {
	ParserWg *sync.WaitGroup
	DbWg     *sync.WaitGroup
	ErrorWg  *sync.WaitGroup
}

type Config struct {
	NumParserWorkers             int
	DbBatchSize                  int
	NumDBWorkersPerReferenceDate int
	ResultsChannelSize           int
}

type FileMap = map[int]string

type SetupReturn struct {
	FileInfo          []FileInfo
	Channels          *ExtractionChannels
	WaitGroups        *ExtractionWaitGroups
	FileMap           *FileMap
	FileErrorsMap     *FileErrorMap
	CreatedPartitions *FirstWritePartition
	Cleanup           func()
}

func (s *SetupReturn) GetValues() ([]FileInfo, *ExtractionChannels, *ExtractionWaitGroups, *FileMap, *FileErrorMap, *FirstWritePartition, func()) {
	return s.FileInfo, s.Channels, s.WaitGroups, s.FileMap, s.FileErrorsMap, s.CreatedPartitions, s.Cleanup
}
