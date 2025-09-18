package ingestion

import (
	"log"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/database"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/parser"
)

type AsyncWorkerConfig struct {
	NumParserWorkers             int
	NumDBWorkersPerReferenceDate int
	DBBatchSize                  int
}

type AsyncWorker struct {
	config    AsyncWorkerConfig
	dbManager database.DBManager
}

func NewAsyncWorker(dbManager database.DBManager, cfg AsyncWorkerConfig) *AsyncWorker {
	return &AsyncWorker{
		dbManager: dbManager,
		config:    cfg,
	}
}

func (w *AsyncWorker) ParserWorker(channels *models.ExtractionChannels, waitGroups *models.ExtractionWaitGroups) {
	defer waitGroups.ParserWg.Done()
	for job := range channels.Jobs {
		log.Printf("Parser worker started job for file %s (ID: %d)\n", job.FilePath, job.FileID)
		err := parser.ParseCSV(job.FilePath, job.FileID, channels.Results, channels.Errors)
		if err != nil {
			channels.Errors <- models.AppError{FileID: job.FileID, Message: "Failed to open or read file", Err: err}
		}
		log.Printf("Parser worker finished job for file %s (ID: %d)\n", job.FilePath, job.FileID)
	}
}

func (w *AsyncWorker) DbWorker(workerId int, stagingTableName string, resultsChan <-chan *models.Trade, errorsChan chan<- models.AppError, waitGroups *models.ExtractionWaitGroups, dbHandler func(*[]*models.Trade, string) error) {
	log.Printf("DB Worker %d: Starting to process trades from channel %s\n", workerId, stagingTableName)
	defer waitGroups.DbWg.Done()
	trades := make([]*models.Trade, 0, w.config.DBBatchSize)

	for result := range resultsChan {
		trades = append(trades, result)
		if len(trades) >= w.config.DBBatchSize {
			log.Printf("DB Worker %d: Inserting batch of %d trades using table %s\n", workerId, len(trades), stagingTableName)
			err := dbHandler(&trades, stagingTableName)
			if err != nil {
				// The batch failed, so report an error for each unique FileID in the batch.
				fileIDs := make(map[int]bool)
				for _, trade := range trades {
					fileIDs[trade.FileID] = true
				}
				for fileID := range fileIDs {
					errorsChan <- models.AppError{FileID: fileID, Message: "Failed to insert batch of trades", Err: err}
				}
			}
			trades = trades[:0] // Clear the slice
		}
	}

	// Insert any remaining trades
	if len(trades) > 0 {
		log.Printf("DB Worker %d: Inserting final batch of %d trades using table %s\n", workerId, len(trades), stagingTableName)
		// Using the DB manager's stored context
		err := dbHandler(&trades, stagingTableName)
		if err != nil {
			// The batch failed, so report an error for each unique FileID in the batch.
			fileIDs := make(map[int]bool)
			for _, trade := range trades {
				fileIDs[trade.FileID] = true
			}
			for fileID := range fileIDs {
				errorsChan <- models.AppError{FileID: fileID, Message: "Failed to insert remaining batch of trades", Err: err}
			}
		}
	}

	log.Printf("DB worker %d finished.", workerId)
}

func (w *AsyncWorker) ErrorWorker(channels *models.ExtractionChannels, fileErrorsMap *models.FileErrorMap, waitGroups *models.ExtractionWaitGroups) {
	defer waitGroups.ErrorWg.Done()
	for appErr := range channels.Errors {
		log.Printf("Caught error: %s\n", appErr.Error())
		// limit the number of errors per file to prevent memory overflow, if more than 100 errors are collected, then file is probably malformed
		if appErr.FileID != -1 && len(fileErrorsMap.Errors) < 100 {
			fileErrorsMap.Mu.Lock()
			fileErrorsMap.Errors[appErr.FileID] = append(fileErrorsMap.Errors[appErr.FileID], appErr)
			fileErrorsMap.Mu.Unlock()
		} else if appErr.FileID != -1 {
			// File has too many errors, skip it, and log for manual inspection
			log.Printf("File %d has too many errors, skipping\n", appErr.FileID)
		}
	}
}

func (w *AsyncWorker) FileStatusWorker(fileErrorsMap *models.FileErrorMap, waitGroups *models.ExtractionWaitGroups, fileMap *models.FileMap) {
	defer waitGroups.ErrorWg.Done()

	fileErrorsMap.Mu.Lock()
	defer fileErrorsMap.Mu.Unlock()

	for fileID := range *fileMap {
		appErrors := fileErrorsMap.Errors[fileID]
		status := database.FILE_STATUS_DONE
		if len(appErrors) > 0 {
			status = database.FILE_STATUS_DONE_WITH_ERRORS
		}

		if err := w.dbManager.UpdateFileStatus(fileID, status, appErrors); err != nil {
			log.Printf("Failed to update status for fileID %d: %v\n", fileID, err)
		}
	}
}
