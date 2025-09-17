package handlers

import (
	"sync"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/models"
)

type FileErrorMap struct {
	errors map[int][]models.AppError
	mu     sync.Mutex
}

type ExtractionChannels struct {
	results map[time.Time]chan *models.Trade
	errors  chan models.AppError
	jobs    chan models.FileProcessingJob
}

type ExtractionWaitGroups struct {
	parserWg *sync.WaitGroup
	dbWg     *sync.WaitGroup
	errorWg  *sync.WaitGroup
}

type Config struct {
	numParserWorkers             int
	dbBatchSize                  int
	numDBWorkersPerReferenceDate int
	resultsChannelSize           int
}

type FileMap = map[int]string
