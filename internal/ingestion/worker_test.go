package ingestion

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestNewAsyncWorker(t *testing.T) {
	dbManager := new(MockDBManager)
	cfg := AsyncWorkerConfig{
		NumDBWorkersPerReferenceDate: 2,
		DBBatchSize:                  100,
	}

	worker := NewAsyncWorker(dbManager, cfg)

	assert.NotNil(t, worker)
	assert.Equal(t, dbManager, worker.dbManager)
	assert.Equal(t, cfg, worker.config)
}

func TestAsyncWorker_WithChannels(t *testing.T) {
	dbManager := new(MockDBManager)
	cfg := AsyncWorkerConfig{}
	worker := NewAsyncWorker(dbManager, cfg)

	channels := &models.ExtractionChannels{}

	worker.WithChannels(channels)

	assert.Equal(t, channels, worker.channels)
}

func TestAsyncWorker_WithWaitGroups(t *testing.T) {
	dbManager := new(MockDBManager)
	cfg := AsyncWorkerConfig{}
	worker := NewAsyncWorker(dbManager, cfg)

	waitGroups := &models.ExtractionWaitGroups{}

	worker.WithWaitGroups(waitGroups)

	assert.Equal(t, waitGroups, worker.waitGroups)
}

func TestAsyncWorker_ErrorWorker(t *testing.T) {
	t.Run("Success case - aggregates errors", func(t *testing.T) {
		dbManager := new(MockDBManager)
		worker := NewAsyncWorker(dbManager, AsyncWorkerConfig{})

		errorsChan := make(chan models.AppError, 2)
		waitGroups := &models.ExtractionWaitGroups{MainWg: &sync.WaitGroup{}}
		fileErrorsMap := &models.FileErrorMap{
			Errors: make(map[int][]models.AppError),
			Mu:     sync.Mutex{},
		}

		worker.WithChannels(&models.ExtractionChannels{Errors: errorsChan}).WithWaitGroups(waitGroups)

		waitGroups.MainWg.Add(1)
		go worker.ErrorWorker(fileErrorsMap)

		errorsChan <- models.AppError{FileID: 1, Message: "error 1"}
		errorsChan <- models.AppError{FileID: 1, Message: "error 2"}
		close(errorsChan)

		waitGroups.MainWg.Wait()

		assert.Len(t, fileErrorsMap.Errors[1], 2, "Should have aggregated 2 errors for FileID 1")
	})

	t.Run("Success case - stops aggregating after 100 errors", func(t *testing.T) {
		dbManager := new(MockDBManager)
		worker := NewAsyncWorker(dbManager, AsyncWorkerConfig{})

		errorsChan := make(chan models.AppError, 101)
		waitGroups := &models.ExtractionWaitGroups{MainWg: &sync.WaitGroup{}}
		fileErrorsMap := &models.FileErrorMap{
			Errors: make(map[int][]models.AppError),
			Mu:     sync.Mutex{},
		}

		worker.WithChannels(&models.ExtractionChannels{Errors: errorsChan}).WithWaitGroups(waitGroups)

		waitGroups.MainWg.Add(1)
		go worker.ErrorWorker(fileErrorsMap)

		for i := 0; i < 101; i++ {
			errorsChan <- models.AppError{FileID: 2, Message: "an error"}
		}
		close(errorsChan)

		waitGroups.MainWg.Wait()

		assert.Len(t, fileErrorsMap.Errors[2], 100, "Should have stopped aggregating at 100 errors")
	})
}

func TestAsyncWorker_DbWorker(t *testing.T) {
	const dbBatchSize = 2

	t.Run("Success case - full batch and final batch", func(t *testing.T) {
		dbManager := new(MockDBManager)
		cfg := AsyncWorkerConfig{DBBatchSize: dbBatchSize}
		worker := NewAsyncWorker(dbManager, cfg)

		resultsChan := make(chan *models.Trade, 3)
		errorsChan := make(chan models.AppError, 1)
		waitGroups := &models.ExtractionWaitGroups{DbWg: &sync.WaitGroup{}}

		var handlerCalled int
		dbHandler := func(trades *[]*models.Trade, stagingTableName string) error {
			handlerCalled++
			if handlerCalled == 1 {
				assert.Len(t, *trades, dbBatchSize)
			} else {
				assert.Len(t, *trades, 1) // Final batch
			}
			return nil
		}

		waitGroups.DbWg.Add(1)
		go worker.DbWorker(1, "staging_table", resultsChan, errorsChan, waitGroups, dbHandler)

		resultsChan <- &models.Trade{FileID: 1}
		resultsChan <- &models.Trade{FileID: 2}
		resultsChan <- &models.Trade{FileID: 3}
		close(resultsChan)

		waitGroups.DbWg.Wait()

		assert.Equal(t, 2, handlerCalled, "DB handler should be called twice")
		assert.Len(t, errorsChan, 0, "No errors should be sent")
	})

	t.Run("Error case - db handler fails", func(t *testing.T) {
		dbManager := new(MockDBManager)
		cfg := AsyncWorkerConfig{DBBatchSize: dbBatchSize}
		worker := NewAsyncWorker(dbManager, cfg)

		resultsChan := make(chan *models.Trade, 2)
		errorsChan := make(chan models.AppError, 2)
		waitGroups := &models.ExtractionWaitGroups{DbWg: &sync.WaitGroup{}}

		dbHandler := func(trades *[]*models.Trade, stagingTableName string) error {
			return errors.New("db insert error")
		}

		waitGroups.DbWg.Add(1)
		go worker.DbWorker(1, "staging_table", resultsChan, errorsChan, waitGroups, dbHandler)

		resultsChan <- &models.Trade{FileID: 10}
		resultsChan <- &models.Trade{FileID: 11}
		close(resultsChan)

		waitGroups.DbWg.Wait()

		assert.Len(t, errorsChan, 2, "Two errors should be sent for the two unique FileIDs")

		errorsReceived := make(map[int]bool)
		for i := 0; i < 2; i++ {
			appErr := <-errorsChan
			errorsReceived[appErr.FileID] = true
		}
		assert.True(t, errorsReceived[10])
		assert.True(t, errorsReceived[11])
	})

	t.Run("Success case - no trades", func(t *testing.T) {
		dbManager := new(MockDBManager)
		cfg := AsyncWorkerConfig{DBBatchSize: dbBatchSize}
		worker := NewAsyncWorker(dbManager, cfg)

		resultsChan := make(chan *models.Trade)
		errorsChan := make(chan models.AppError, 1)
		waitGroups := &models.ExtractionWaitGroups{DbWg: &sync.WaitGroup{}}

		var handlerCalled bool
		dbHandler := func(trades *[]*models.Trade, stagingTableName string) error {
			handlerCalled = true
			return nil
		}

		waitGroups.DbWg.Add(1)
		go worker.DbWorker(1, "staging_table", resultsChan, errorsChan, waitGroups, dbHandler)

		close(resultsChan)

		waitGroups.DbWg.Wait()

		assert.False(t, handlerCalled, "DB handler should not be called")
	})
}

func TestAsyncWorker_ParserWorker(t *testing.T) {
	t.Run("Success case", func(t *testing.T) {
		// Create a temporary file for the test
		tmpfile, err := os.CreateTemp("", "parser_test_*.csv")
		assert.NoError(t, err)
		defer os.Remove(tmpfile.Name())

		// Write header and a data line to the temp file
		header := "DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;QuantidadeNegociada;HoraFechamento;CodigoIdentificadorNegocio;TipoSessaoPregao;DataNegocio;CodigoParticipanteComprador;CodigoParticipanteVendedor\n"
		data := "2023-10-10;PETR4;I;30,5;100;17:30:00;12345;1;2023-10-10;123;456\n"
		_, err = tmpfile.WriteString(header + data)
		assert.NoError(t, err)
		tmpfile.Close()

		dbManager := new(MockDBManager)
		worker := NewAsyncWorker(dbManager, AsyncWorkerConfig{})

		dateKey, _ := time.Parse("2006-01-02", "2023-10-10")
		resultsChan := make(chan *models.Trade, 1)

		channels := &models.ExtractionChannels{
			Jobs:    make(chan models.FileProcessingJob, 1),
			Results: map[time.Time]chan *models.Trade{dateKey: resultsChan},
			Errors:  make(chan models.AppError, 1),
		}
		waitGroups := &models.ExtractionWaitGroups{
			ParserWg: &sync.WaitGroup{},
		}

		worker.WithChannels(channels).WithWaitGroups(waitGroups)

		waitGroups.ParserWg.Add(1)
		go worker.ParserWorker()

		channels.Jobs <- models.FileProcessingJob{FilePath: tmpfile.Name(), FileID: 1}
		close(channels.Jobs)

		select {
		case trade := <-resultsChan:
			assert.Equal(t, "PETR4", trade.Ticker)
			assert.Equal(t, 30.5, trade.Price)
			assert.Equal(t, int64(100), trade.Quantity)
			assert.Equal(t, 1, trade.FileID)
		case appErr := <-channels.Errors:
			t.Fatalf("Expected no error, but got: %v", appErr)
		case <-time.After(1 * time.Second):
			t.Fatal("Test timed out waiting for result")
		}

		waitGroups.ParserWg.Wait()
	})

	t.Run("Error case - file not found", func(t *testing.T) {
		dbManager := new(MockDBManager)
		worker := NewAsyncWorker(dbManager, AsyncWorkerConfig{})

		channels := &models.ExtractionChannels{
			Jobs:    make(chan models.FileProcessingJob, 1),
			Results: make(map[time.Time]chan *models.Trade),
			Errors:  make(chan models.AppError, 1),
		}
		waitGroups := &models.ExtractionWaitGroups{
			ParserWg: &sync.WaitGroup{},
		}

		worker.WithChannels(channels).WithWaitGroups(waitGroups)

		waitGroups.ParserWg.Add(1)
		go worker.ParserWorker()

		channels.Jobs <- models.FileProcessingJob{FilePath: "/non/existent/file.csv", FileID: 2}
		close(channels.Jobs)

		select {
		case <-channels.Results[time.Time{}]: // Reading from a nil channel blocks forever, which is fine for this case
			t.Fatal("Expected an error, but got a trade")
		case appErr := <-channels.Errors:
			assert.Equal(t, 2, appErr.FileID)
			assert.Contains(t, appErr.Message, "Failed to open file")
		case <-time.After(1 * time.Second):
			t.Fatal("Test timed out waiting for error")
		}

		waitGroups.ParserWg.Wait()
	})
}
