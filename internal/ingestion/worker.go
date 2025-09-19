package ingestion

import (
	"fmt"
	"log"
	"sync"

	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/database"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/parser"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/pkg/checksum"
)

type Runner[T any] struct {
	Run T
}

type AsyncWorkerConfig struct {
	NumDBWorkersPerReferenceDate int
	DBBatchSize                  int
}

// Worker defines the interface for asynchronous processing tasks.
type Worker interface {
	WithChannels(channels *models.ExtractionChannels) Worker
	WithWaitGroups(waitGroups *models.ExtractionWaitGroups) Worker
	SetupErrorWorker() (Runner[func(*models.FileErrorMap)], *sync.WaitGroup, error)
	SetupParserWorkers(numberOfWorkers int) (Runner[func()], *sync.WaitGroup, error)
	SetupDBWorkers(numDBWorkersPerReferenceDate int) (Runner[func(func(*[]*models.Trade, string) error) error], *sync.WaitGroup, error)
	SetupJobDispatcherWorker(fileInfos []models.FileInfo, fileMap map[int]string) (Runner[func()], *sync.WaitGroup, error)
}

type AsyncWorker struct {
	config     AsyncWorkerConfig
	dbManager  database.DBManager
	channels   *models.ExtractionChannels
	waitGroups *models.ExtractionWaitGroups
}

func NewAsyncWorker(dbManager database.DBManager, cfg AsyncWorkerConfig) *AsyncWorker {
	return &AsyncWorker{
		dbManager: dbManager,
		config:    cfg,
	}
}

func (w *AsyncWorker) WithChannels(channels *models.ExtractionChannels) Worker {
	w.channels = channels
	return w
}

func (w *AsyncWorker) WithWaitGroups(waitGroups *models.ExtractionWaitGroups) Worker {
	w.waitGroups = waitGroups
	return w
}

func (w *AsyncWorker) ParserWorker() {
	defer w.waitGroups.ParserWg.Done()
	for job := range w.channels.Jobs {
		log.Printf("Parser worker started job for file %s (ID: %d)\n", job.FilePath, job.FileID)
		err := parser.ParseCSV(job.FilePath, job.FileID, w.channels.Results, w.channels.Errors)
		if err != nil {
			w.channels.Errors <- models.AppError{FileID: job.FileID, Message: "Failed to open or read file", Err: err}
		}
		log.Printf("Parser worker finished job for file %s (ID: %d)\n", job.FilePath, job.FileID)
	}
}

func (w *AsyncWorker) SetupParserWorkers(numberOfWorkers int) (Runner[func()], *sync.WaitGroup, error) {
	return Runner[func()]{
		Run: func() {
			for i := 1; i <= numberOfWorkers; i++ {
				w.waitGroups.ParserWg.Add(1)
				go w.ParserWorker()
			}
		},
	}, w.waitGroups.ParserWg, nil
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

func (w *AsyncWorker) SetupDBWorkers(numDBWorkersPerReferenceDate int) (Runner[func(func(*[]*models.Trade, string) error) error], *sync.WaitGroup, error) {
	return Runner[func(func(*[]*models.Trade, string) error) error]{
		Run: func(dbHandler func(*[]*models.Trade, string) error) error {
			workerCounter := 1
			for date, resultsChan := range w.channels.Results {
				log.Printf("Starting %d DB workers for date %s", numDBWorkersPerReferenceDate, date.Format("2006-01-02"))
				for i := 1; i <= numDBWorkersPerReferenceDate; i++ {
					workerId := workerCounter
					stagingTableName := fmt.Sprintf("trade_records_staging_worker_%d", workerId)
					w.waitGroups.DbWg.Add(1)
					go w.DbWorker(workerId, stagingTableName, resultsChan, w.channels.Errors, w.waitGroups, dbHandler)
					workerCounter++
				}
			}
			return nil
		},
	}, w.waitGroups.DbWg, nil
}

func (w *AsyncWorker) ErrorWorker(fileErrorsMap *models.FileErrorMap) {
	defer w.waitGroups.MainWg.Done()
	for appErr := range w.channels.Errors {
		log.Printf("Caught error: %s\n", appErr.Error())
		// limit the number of errors per file to prevent memory overflow, if more than 100 errors are collected, then file is probably malformed
		if appErr.FileID != -1 && len(fileErrorsMap.Errors[appErr.FileID]) < 100 {
			fileErrorsMap.Mu.Lock()
			fileErrorsMap.Errors[appErr.FileID] = append(fileErrorsMap.Errors[appErr.FileID], appErr)
			fileErrorsMap.Mu.Unlock()
		} else if appErr.FileID != -1 {
			// File has too many errors, skip it, and log for manual inspection
			log.Printf("File %d has too many errors, skipping\n", appErr.FileID)
		}
	}
}

func (w *AsyncWorker) PreprocessAndDispatchJobs(
	fileInfos []models.FileInfo,
	fileMap map[int]string,
) {
	defer close(w.channels.Jobs)
	defer w.waitGroups.MainWg.Done()

	for _, fileInfo := range fileInfos {
		checksum, err := checksum.GetFileChecksum(fileInfo.Path)
		if err != nil {
			log.Printf("ERROR: Failed to calculate checksum for %s: %v. Skipping file.", fileInfo.Path, err)
			continue
		}

		isProcessed, err := w.dbManager.IsFileAlreadyProcessed(checksum)
		if err != nil {
			log.Printf("ERROR: Failed to check if file %s is already processed: %v. Skipping file.", fileInfo.Path, err)
			continue
		}
		if isProcessed {
			log.Printf("INFO: File %s (checksum: %s) has already been processed. Skipping.", fileInfo.Path, checksum)
			continue
		}

		fileID, err := w.dbManager.InsertFileRecord(
			fileInfo.Path,
			time.Now(),
			database.FILE_STATUS_PROCESSING,
			checksum,
			fileInfo.ReferenceDate,
		)
		if err != nil {
			log.Printf("ERROR: Failed to insert file record for %s: %v. Skipping file.", fileInfo.Path, err)
			continue
		}

		fileMap[fileID] = fileInfo.Path

		log.Printf("Dispatching job for file: %s (FileID: %d)", fileInfo.Path, fileID)
		w.channels.Jobs <- models.FileProcessingJob{FilePath: fileInfo.Path, FileID: fileID}
	}
}

func (w *AsyncWorker) SetupJobDispatcherWorker(fileInfos []models.FileInfo, fileMap map[int]string) (Runner[func()], *sync.WaitGroup, error) {
	return Runner[func()]{
		Run: func() {
			w.waitGroups.MainWg.Add(1)
			go w.PreprocessAndDispatchJobs(fileInfos, fileMap)
		},
	}, w.waitGroups.MainWg, nil
}

func (w *AsyncWorker) SetupErrorWorker() (Runner[func(*models.FileErrorMap)], *sync.WaitGroup, error) {
	return Runner[func(*models.FileErrorMap)]{
		Run: func(fileErrorsMap *models.FileErrorMap) {
			w.waitGroups.MainWg.Add(1)
			go w.ErrorWorker(fileErrorsMap)
		},
	}, w.waitGroups.MainWg, nil
}
