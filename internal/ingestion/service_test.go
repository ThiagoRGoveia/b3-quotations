package ingestion

import (
	"sync"
	"testing"
	"time"

	"errors"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/config"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
	"github.com/stretchr/testify/mock"
)

// MockDBManager is a mock implementation of the DBManager interface.
type MockDBManager struct {
	mock.Mock
}

func (m *MockDBManager) DropTradeRecordIndexes() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockDBManager) CreateTradeRecordIndexes() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockDBManager) CreatePartitionsForDates(dates []time.Time) (*models.FirstWritePartition, error) {
	args := m.Called(dates)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.FirstWritePartition), args.Error(1)
}

func (m *MockDBManager) CreateWorkerStagingTables(numTables int) ([]string, error) {
	args := m.Called(numTables)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockDBManager) DropWorkerStagingTable(tableName string) error {
	args := m.Called(tableName)
	return args.Error(0)
}

func (m *MockDBManager) InsertAllStagingTableData(trades []*models.Trade, stagingTableName string) error {
	args := m.Called(trades, stagingTableName)
	return args.Error(0)
}

func (m *MockDBManager) InsertDiffFromStagingTable(trades []*models.Trade, stagingTableName string) error {
	args := m.Called(trades, stagingTableName)
	return args.Error(0)
}

func (m *MockDBManager) CreateFileRecordsTable() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockDBManager) CreateTradeRecordsTable() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockDBManager) InsertFileRecord(fileName string, date time.Time, status string, checksum string, referenceDate time.Time) (int, error) {
	args := m.Called(fileName, date, status, checksum, referenceDate)
	return args.Int(0), args.Error(1)
}

func (m *MockDBManager) UpdateFileStatus(fileID int, status string, errors any) error {
	args := m.Called(fileID, status, errors)
	return args.Error(0)
}

func (m *MockDBManager) IsFileAlreadyProcessed(checksum string) (bool, error) {
	args := m.Called(checksum)
	return args.Bool(0), args.Error(1)
}

func (m *MockDBManager) GetTickerInfo(ticker string, startDate time.Time) (*models.TickerInfo, error) {
	args := m.Called(ticker, startDate)
	return args.Get(0).(*models.TickerInfo), args.Error(1)
}

// MockWorker is a mock implementation of the Worker interface.
type MockWorker struct {
	mock.Mock
}

func (m *MockWorker) WithChannels(channels *models.ExtractionChannels) Worker {
	m.Called(channels)
	return m
}

func (m *MockWorker) WithWaitGroups(waitGroups *models.ExtractionWaitGroups) Worker {
	m.Called(waitGroups)
	return m
}

func (m *MockWorker) SetupErrorWorker() (Runner[func(*models.FileErrorMap)], *sync.WaitGroup, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return Runner[func(*models.FileErrorMap)]{}, nil, args.Error(2)
	}
	return args.Get(0).(Runner[func(*models.FileErrorMap)]), args.Get(1).(*sync.WaitGroup), args.Error(2)
}

func (m *MockWorker) SetupParserWorkers(numWorkers int) (Runner[func()], *sync.WaitGroup, error) {
	args := m.Called(numWorkers)
	if args.Get(0) == nil {
		return Runner[func()]{}, nil, args.Error(2)
	}
	return args.Get(0).(Runner[func()]), args.Get(1).(*sync.WaitGroup), args.Error(2)
}

func (m *MockWorker) SetupDBWorkers(numWorkersPerDate int) (Runner[func(func(*[]*models.Trade, string) error) error], *sync.WaitGroup, error) {
	args := m.Called(numWorkersPerDate)
	if args.Get(0) == nil {
		return Runner[func(func(*[]*models.Trade, string) error) error]{}, nil, args.Error(2)
	}
	return args.Get(0).(Runner[func(func(*[]*models.Trade, string) error) error]), args.Get(1).(*sync.WaitGroup), args.Error(2)
}

func (m *MockWorker) SetupJobDispatcherWorker(fileInfos []models.FileInfo, fileMap map[int]string) (Runner[func()], *sync.WaitGroup, error) {
	args := m.Called(fileInfos, fileMap)
	if args.Get(0) == nil {
		return Runner[func()]{}, nil, args.Error(2)
	}
	return args.Get(0).(Runner[func()]), args.Get(1).(*sync.WaitGroup), args.Error(2)
}

// MockProcessor is a mock implementation of the Processor interface.
type MockProcessor struct {
	mock.Mock
}

func (m *MockProcessor) ScanForFiles(path string) ([]models.FileInfo, error) {
	args := m.Called(path)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]models.FileInfo), args.Error(1)
}

func (m *MockProcessor) UpdateFileStatus(fileErrorsMap *models.FileErrorMap, fileMap *models.FileMap) error {
	args := m.Called(fileErrorsMap, fileMap)
	return args.Error(0)
}

// MockSetup is a mock implementation of the ISetup interface.

type MockSetup struct {
	mock.Mock
}

func (m *MockSetup) build() (models.SetupReturn, error) {
	args := m.Called()
	return args.Get(0).(models.SetupReturn), args.Error(1)
}

func BuildTestSetup() (string, *MockDBManager, *MockWorker, *MockProcessor, *MockSetup, models.SetupReturn, config.Config, time.Time) {
	const path = "some/path"
	dbManager := new(MockDBManager)
	worker := new(MockWorker)
	processor := new(MockProcessor)
	setup := new(MockSetup)

	// fixed time

	cfg := config.Config{
		NumParserWorkers:             1,
		NumDBWorkersPerReferenceDate: 3,
		ResultsChannelSize:           100,
	}

	fileMap := make(map[int]string)
	setupReturn := models.SetupReturn{
		Channels: &models.ExtractionChannels{
			Results: make(map[time.Time]chan *models.Trade),
			Errors:  make(chan models.AppError, 100),
			Jobs:    make(chan models.FileProcessingJob, 100),
		},
		WaitGroups:    &models.ExtractionWaitGroups{ParserWg: &sync.WaitGroup{}, DbWg: &sync.WaitGroup{}, MainWg: &sync.WaitGroup{}},
		FileMap:       &fileMap,
		FileErrorsMap: &models.FileErrorMap{Errors: make(map[int][]models.AppError)},
	}
	return path, dbManager, worker, processor, setup, setupReturn, cfg, time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC)
}

func TestIngestionService_Execute(t *testing.T) {
	t.Run("Expect: Execute to run successfully", func(t *testing.T) {
		path, dbManager, worker, processor, setup, setupReturn, cfg, date := BuildTestSetup()
		numberOfUniqueDates := 1
		scanResult := []models.FileInfo{{ReferenceDate: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC)}}
		setup.On("build").Return(setupReturn, nil).Once()
		dbManager.On("DropTradeRecordIndexes").Return(nil).Once()
		dbManager.On("CreateTradeRecordIndexes").Return(nil).Once()
		processor.On("ScanForFiles", path).Return(scanResult, nil).Once()
		dbManager.On("CreatePartitionsForDates", []time.Time{date}).Return(&models.FirstWritePartition{}, nil).Once()
		totalDBWorkers := numberOfUniqueDates * cfg.NumDBWorkersPerReferenceDate
		dbManager.On("CreateWorkerStagingTables", totalDBWorkers).Return([]string{"staging_table_1", "staging_table_2", "staging_table_3"}, nil).Once()
		dbManager.On("DropWorkerStagingTable", "staging_table_1").Return(nil).Once()
		dbManager.On("DropWorkerStagingTable", "staging_table_2").Return(nil).Once()
		dbManager.On("DropWorkerStagingTable", "staging_table_3").Return(nil).Once()
		worker.On("WithChannels", setupReturn.Channels).Return(worker).Once()
		worker.On("WithWaitGroups", setupReturn.WaitGroups).Return(worker).Once()
		dispatcherRunner := Runner[func()]{Run: func() {}}
		worker.On("SetupJobDispatcherWorker", scanResult, *setupReturn.FileMap).Return(dispatcherRunner, &sync.WaitGroup{}, nil).Once()
		processor.On("UpdateFileStatus", setupReturn.FileErrorsMap, setupReturn.FileMap).Return(nil).Once()

		errorRunner := Runner[func(*models.FileErrorMap)]{Run: func(fem *models.FileErrorMap) {}}
		worker.On("SetupErrorWorker").Return(errorRunner, &sync.WaitGroup{}, nil).Once()

		parserRunner := Runner[func()]{Run: func() {}}
		worker.On("SetupParserWorkers", cfg.NumParserWorkers).Return(parserRunner, &sync.WaitGroup{}, nil).Once()

		dbRunner := Runner[func(func(*[]*models.Trade, string) error) error]{Run: func(handler func(*[]*models.Trade, string) error) error { return nil }}
		worker.On("SetupDBWorkers", cfg.NumDBWorkersPerReferenceDate).Return(dbRunner, &sync.WaitGroup{}, nil).Once()

		service := NewIngestionService(dbManager, setup, worker, processor, cfg)
		err := service.Execute(path)

		if err != nil {
			t.Errorf("Did not expect an error, but got: %v", err)
		}

		dbManager.AssertExpectations(t)
		worker.AssertExpectations(t)
		processor.AssertExpectations(t)
		setup.AssertExpectations(t)
	})

	t.Run("Expect: Error to be returned when fileProcessor.ScanForFiles() fails", func(t *testing.T) {
		path, dbManager, worker, processor, setup, _, cfg, _ := BuildTestSetup()
		setup.On("build").Return(models.SetupReturn{}, errors.New("build error")).Once()

		service := NewIngestionService(dbManager, setup, worker, processor, cfg)
		err := service.Execute(path)

		if err == nil {
			t.Errorf("Expected an error, but got nil")
		}

		setup.AssertExpectations(t)
		dbManager.AssertNotCalled(t, "DropTradeRecordIndexes")
		processor.AssertNotCalled(t, "ScanForFiles")
	})

	t.Run("Expect: Error to be returned when ScanForFilesError() fails", func(t *testing.T) {
		path, dbManager, worker, processor, setup, setupReturn, cfg, _ := BuildTestSetup()
		setup.On("build").Return(setupReturn, nil).Once()
		processor.On("ScanForFiles", path).Return(nil, errors.New("scan error")).Once()

		service := NewIngestionService(dbManager, setup, worker, processor, cfg)
		err := service.Execute(path)

		if err == nil {
			t.Errorf("Expected an error, but got nil")
		}

		setup.AssertExpectations(t)
		processor.AssertExpectations(t)
		dbManager.AssertNotCalled(t, "CreatePartitionsForDates")
	})

	t.Run("Expect: Error to be returned when setupDatabase() fails due to a partition creation error", func(t *testing.T) {
		path, dbManager, worker, processor, setup, setupReturn, cfg, date := BuildTestSetup()
		scanResult := []models.FileInfo{{ReferenceDate: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC)}}
		setup.On("build").Return(setupReturn, nil).Once()
		processor.On("ScanForFiles", path).Return(scanResult, nil).Once()
		dbManager.On("CreatePartitionsForDates", []time.Time{date}).Return(nil, errors.New("partition error")).Once()

		service := NewIngestionService(dbManager, setup, worker, processor, cfg)
		err := service.Execute(path)

		if err == nil {
			t.Errorf("Expected an error, but got nil")
		}

		setup.AssertExpectations(t)
		processor.AssertExpectations(t)
		dbManager.AssertExpectations(t)
		dbManager.AssertNotCalled(t, "DropTradeRecordIndexes")
	})

	t.Run("Expect: Error to be returned when setupDatabase() fails due to a partition creation error", func(t *testing.T) {
		path, dbManager, worker, processor, setup, setupReturn, cfg, date := BuildTestSetup()
		scanResult := []models.FileInfo{{ReferenceDate: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC)}}
		setup.On("build").Return(setupReturn, nil).Once()
		processor.On("ScanForFiles", path).Return(scanResult, nil).Once()
		dbManager.On("CreatePartitionsForDates", []time.Time{date}).Return(&models.FirstWritePartition{}, nil).Once()
		dbManager.On("CreateWorkerStagingTables", cfg.NumDBWorkersPerReferenceDate).Return([]string{}, nil).Once()
		dbManager.On("DropTradeRecordIndexes").Return(nil).Once()
		worker.On("WithChannels", setupReturn.Channels).Return(worker).Once()
		worker.On("WithWaitGroups", setupReturn.WaitGroups).Return(worker).Once()
		worker.On("SetupJobDispatcherWorker", scanResult, *setupReturn.FileMap).Return(nil, nil, errors.New("dispatcher error")).Once()
		dbManager.On("CreateTradeRecordIndexes").Return(nil).Once()

		service := NewIngestionService(dbManager, setup, worker, processor, cfg)
		err := service.Execute(path)

		if err == nil {
			t.Errorf("Expected an error, but got nil")
		}

		setup.AssertExpectations(t)
		processor.AssertExpectations(t)
		dbManager.AssertExpectations(t)
		worker.AssertExpectations(t)
		worker.AssertNotCalled(t, "SetupErrorWorker")
	})

	t.Run("Expect: Error to be returned when setupErrorWorker() fails", func(t *testing.T) {
		path, dbManager, worker, processor, setup, setupReturn, cfg, date := BuildTestSetup()
		scanResult := []models.FileInfo{{ReferenceDate: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC)}}

		setup.On("build").Return(setupReturn, nil).Once()
		processor.On("ScanForFiles", path).Return(scanResult, nil).Once()
		dbManager.On("CreatePartitionsForDates", []time.Time{date}).Return(&models.FirstWritePartition{}, nil).Once()
		dbManager.On("CreateWorkerStagingTables", cfg.NumDBWorkersPerReferenceDate).Return([]string{}, nil).Once()
		dbManager.On("DropTradeRecordIndexes").Return(nil).Once()
		worker.On("WithChannels", setupReturn.Channels).Return(worker).Once()
		worker.On("WithWaitGroups", setupReturn.WaitGroups).Return(worker).Once()
		worker.On("SetupJobDispatcherWorker", scanResult, *setupReturn.FileMap).Return(Runner[func()]{Run: func() {}}, &sync.WaitGroup{}, nil).Once()
		worker.On("SetupErrorWorker").Return(nil, nil, errors.New("error worker error")).Once()
		dbManager.On("CreateTradeRecordIndexes").Return(nil).Once()

		service := NewIngestionService(dbManager, setup, worker, processor, cfg)
		err := service.Execute(path)

		if err == nil {
			t.Errorf("Expected an error, but got nil")
		}

		setup.AssertExpectations(t)
		processor.AssertExpectations(t)
		dbManager.AssertExpectations(t)
		worker.AssertExpectations(t)
		worker.AssertNotCalled(t, "SetupParserWorkers")
	})

	t.Run("Expect: Error to be returned when setupParserWorkers() fails", func(t *testing.T) {
		path, dbManager, worker, processor, setup, setupReturn, cfg, date := BuildTestSetup()
		scanResult := []models.FileInfo{{ReferenceDate: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC)}}

		setup.On("build").Return(setupReturn, nil).Once()
		processor.On("ScanForFiles", path).Return(scanResult, nil).Once()
		dbManager.On("CreatePartitionsForDates", []time.Time{date}).Return(&models.FirstWritePartition{}, nil).Once()
		dbManager.On("CreateWorkerStagingTables", cfg.NumDBWorkersPerReferenceDate).Return([]string{}, nil).Once()
		dbManager.On("DropTradeRecordIndexes").Return(nil).Once()
		worker.On("WithChannels", setupReturn.Channels).Return(worker).Once()
		worker.On("WithWaitGroups", setupReturn.WaitGroups).Return(worker).Once()
		worker.On("SetupJobDispatcherWorker", scanResult, *setupReturn.FileMap).Return(Runner[func()]{Run: func() {}}, &sync.WaitGroup{}, nil).Once()
		worker.On("SetupErrorWorker").Return(Runner[func(*models.FileErrorMap)]{Run: func(_ *models.FileErrorMap) {}}, &sync.WaitGroup{}, nil).Once()
		worker.On("SetupParserWorkers", cfg.NumParserWorkers).Return(nil, nil, errors.New("parser error")).Once()
		dbManager.On("CreateTradeRecordIndexes").Return(nil).Once()

		service := NewIngestionService(dbManager, setup, worker, processor, cfg)
		err := service.Execute(path)

		if err == nil {
			t.Errorf("Expected an error, but got nil")
		}

		setup.AssertExpectations(t)
		processor.AssertExpectations(t)
		dbManager.AssertExpectations(t)
		worker.AssertExpectations(t)
		worker.AssertNotCalled(t, "SetupDBWorkers")
	})

	t.Run("Expect: Error to be returned when setupDBWorkers() fails", func(t *testing.T) {
		path, dbManager, worker, processor, setup, setupReturn, cfg, date := BuildTestSetup()
		scanResult := []models.FileInfo{{ReferenceDate: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC)}}

		setup.On("build").Return(setupReturn, nil).Once()
		processor.On("ScanForFiles", path).Return(scanResult, nil).Once()
		dbManager.On("CreatePartitionsForDates", []time.Time{date}).Return(&models.FirstWritePartition{}, nil).Once()
		dbManager.On("CreateWorkerStagingTables", cfg.NumDBWorkersPerReferenceDate).Return([]string{}, nil).Once()
		dbManager.On("DropTradeRecordIndexes").Return(nil).Once()
		worker.On("WithChannels", setupReturn.Channels).Return(worker).Once()
		worker.On("WithWaitGroups", setupReturn.WaitGroups).Return(worker).Once()
		worker.On("SetupJobDispatcherWorker", scanResult, *setupReturn.FileMap).Return(Runner[func()]{Run: func() {}}, &sync.WaitGroup{}, nil).Once()
		worker.On("SetupErrorWorker").Return(Runner[func(*models.FileErrorMap)]{Run: func(_ *models.FileErrorMap) {}}, &sync.WaitGroup{}, nil).Once()
		worker.On("SetupParserWorkers", cfg.NumParserWorkers).Return(Runner[func()]{Run: func() {}}, &sync.WaitGroup{}, nil).Once()
		worker.On("SetupDBWorkers", cfg.NumDBWorkersPerReferenceDate).Return(nil, nil, errors.New("db worker error")).Once()
		dbManager.On("CreateTradeRecordIndexes").Return(nil).Once()

		service := NewIngestionService(dbManager, setup, worker, processor, cfg)
		err := service.Execute(path)

		if err == nil {
			t.Errorf("Expected an error, but got nil")
		}

		setup.AssertExpectations(t)
		processor.AssertExpectations(t)
		dbManager.AssertExpectations(t)
		worker.AssertExpectations(t)
	})

	t.Run("Expect: Error to be returned when DBWorkersRunner() fails", func(t *testing.T) {
		path, dbManager, worker, processor, setup, setupReturn, cfg, date := BuildTestSetup()
		scanResult := []models.FileInfo{{ReferenceDate: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC)}}

		setup.On("build").Return(setupReturn, nil).Once()
		processor.On("ScanForFiles", path).Return(scanResult, nil).Once()
		dbManager.On("CreatePartitionsForDates", []time.Time{date}).Return(&models.FirstWritePartition{}, nil).Once()
		dbManager.On("CreateWorkerStagingTables", cfg.NumDBWorkersPerReferenceDate).Return([]string{}, nil).Once()
		dbManager.On("DropTradeRecordIndexes").Return(nil).Once()
		worker.On("WithChannels", setupReturn.Channels).Return(worker).Once()
		worker.On("WithWaitGroups", setupReturn.WaitGroups).Return(worker).Once()
		worker.On("SetupJobDispatcherWorker", scanResult, *setupReturn.FileMap).Return(Runner[func()]{Run: func() {}}, &sync.WaitGroup{}, nil).Once()
		worker.On("SetupErrorWorker").Return(Runner[func(*models.FileErrorMap)]{Run: func(_ *models.FileErrorMap) {}}, &sync.WaitGroup{}, nil).Once()
		worker.On("SetupParserWorkers", cfg.NumParserWorkers).Return(Runner[func()]{Run: func() {}}, &sync.WaitGroup{}, nil).Once()
		dbRunner := Runner[func(func(*[]*models.Trade, string) error) error]{Run: func(handler func(*[]*models.Trade, string) error) error { return errors.New("db runner error") }}
		worker.On("SetupDBWorkers", cfg.NumDBWorkersPerReferenceDate).Return(dbRunner, &sync.WaitGroup{}, nil).Once()
		dbManager.On("CreateTradeRecordIndexes").Return(nil).Once()

		service := NewIngestionService(dbManager, setup, worker, processor, cfg)
		err := service.Execute(path)

		if err == nil {
			t.Errorf("Expected an error, but got nil")
		}

		setup.AssertExpectations(t)
		processor.AssertExpectations(t)
		dbManager.AssertExpectations(t)
		worker.AssertExpectations(t)
	})
}
