package ingestion

import (
	"sync"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
)

type ISetup interface {
	build() (models.SetupReturn, error)
}

type Setup struct{}

// Instantiate all channels and data structure we will use in the concurrent ingestion process
// Its useful to have it in a separated struct to be able to leverage DI for testing
func (h Setup) build() (models.SetupReturn, error) {
	jobs := make(chan models.FileProcessingJob, 100)
	errors := make(chan models.AppError, 100)

	channels := models.ExtractionChannels{
		Results: make(map[time.Time]chan *models.Trade),
		Errors:  errors,
		Jobs:    jobs,
	}

	var parserWg, dbWg, errorWg sync.WaitGroup
	fileMap := make(map[int]string)
	fileErrorsMap := models.FileErrorMap{Errors: make(map[int][]models.AppError)}
	return models.SetupReturn{
		Channels:      &channels,
		WaitGroups:    &models.ExtractionWaitGroups{ParserWg: &parserWg, DbWg: &dbWg, MainWg: &errorWg},
		FileMap:       &fileMap,
		FileErrorsMap: &fileErrorsMap,
	}, nil
}
