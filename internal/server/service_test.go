package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockDBManager struct {
	mock.Mock
}

func (m *MockDBManager) DropTradeRecordIndexes() error {
	return nil
}

func (m *MockDBManager) CreateTradeRecordIndexes() error {
	return nil
}

func (m *MockDBManager) CreatePartitionsForDates(dates []time.Time) (*models.FirstWritePartition, error) {
	return nil, nil
}

func (m *MockDBManager) CreateWorkerStagingTables(numTables int) ([]string, error) {
	return nil, nil
}

func (m *MockDBManager) DropWorkerStagingTable(tableName string) error {
	return nil
}

func (m *MockDBManager) InsertAllStagingTableData(trades []*models.Trade, stagingTableName string) error {
	return nil
}

func (m *MockDBManager) InsertDiffFromStagingTable(trades []*models.Trade, stagingTableName string) error {
	return nil
}

func (m *MockDBManager) CreateFileRecordsTable() error {
	return nil
}

func (m *MockDBManager) CreateTradeRecordsTable() error {
	return nil
}

func (m *MockDBManager) InsertFileRecord(fileName string, date time.Time, status string, checksum string, referenceDate time.Time) (int, error) {
	return 0, nil
}

func (m *MockDBManager) UpdateFileStatus(fileID int, status string, errors any) error {
	return nil
}

func (m *MockDBManager) IsFileAlreadyProcessed(checksum string) (bool, error) {
	return false, nil
}

func (m *MockDBManager) GetTickerInfo(ticker string, startDate time.Time) (*models.TickerInfo, error) {
	args := m.Called(ticker, startDate)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.TickerInfo), args.Error(1)
}

func TestTickerService_GetTickerInfo(t *testing.T) {
	t.Run("should return ticker information successfully", func(t *testing.T) {
		dbManager := new(MockDBManager)
		service := NewTickerService(dbManager)

		ticker := "PETR4"
		expectedTickerInfo := &models.TickerInfo{
			Ticker:         ticker,
			MaxRangeValue:  100.0,
			MaxDailyVolume: 1000000,
		}

		dbManager.On("GetTickerInfo", ticker, mock.AnythingOfType("time.Time")).Return(expectedTickerInfo, nil).Once()

		req := httptest.NewRequest("GET", "/tickers/"+ticker, nil)
		rr := httptest.NewRecorder()

		service.GetTickerInfo(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		var actualTickerInfo models.TickerInfo
		err := json.NewDecoder(rr.Body).Decode(&actualTickerInfo)
		assert.NoError(t, err)
		assert.Equal(t, *expectedTickerInfo, actualTickerInfo)

		dbManager.AssertExpectations(t)
	})

	t.Run("should return error when ticker is not provided", func(t *testing.T) {
		dbManager := new(MockDBManager)
		service := NewTickerService(dbManager)

		req := httptest.NewRequest("GET", "/tickers/", nil)
		rr := httptest.NewRecorder()

		service.GetTickerInfo(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("should return error for invalid date format", func(t *testing.T) {
		dbManager := new(MockDBManager)
		service := NewTickerService(dbManager)

		req := httptest.NewRequest("GET", "/tickers/PETR4?data_inicio=invalid-date", nil)
		rr := httptest.NewRecorder()

		service.GetTickerInfo(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("should return error when db manager fails", func(t *testing.T) {
		dbManager := new(MockDBManager)
		service := NewTickerService(dbManager)

		ticker := "PETR4"
		dbManager.On("GetTickerInfo", ticker, mock.AnythingOfType("time.Time")).Return(nil, errors.New("db error")).Once()

		req := httptest.NewRequest("GET", "/tickers/"+ticker, nil)
		rr := httptest.NewRecorder()

		service.GetTickerInfo(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)

		dbManager.AssertExpectations(t)
	})
}

func Test_getPastBusinessDay(t *testing.T) {
	// Here we are mocking the time.Now function
	originalTimeNow := timeNow
	defer func() { timeNow = originalTimeNow }()

	t.Run("should return the correct past business day", func(t *testing.T) {
		// Mock the time.Now function to return a fixed date
		fixedDate := time.Date(2023, 10, 11, 0, 0, 0, 0, time.UTC) // Wednesday
		timeNow = func() time.Time { return fixedDate }

		// Calculate the expected date (7 business days before Oct 11, 2023)
		// Oct 11 is a Wednesday, going back 7 business days:
		// Oct 10 (Tue), Oct 9 (Mon), Oct 6 (Fri), Oct 5 (Thu), Oct 4 (Wed), Oct 3 (Tue), Oct 2 (Mon)
		expected := time.Date(2023, 10, 2, 0, 0, 0, 0, time.UTC)

		actual := getPastBusinessDay(7)

		assert.Equal(t, expected, actual, "The dates should match exactly")
	})

	t.Run("should handle weekends correctly", func(t *testing.T) {
		// Mock the time.Now function to return a Monday
		fixedDate := time.Date(2023, 10, 9, 0, 0, 0, 0, time.UTC) // Monday
		timeNow = func() time.Time { return fixedDate }

		// Going back 3 business days from Monday Oct 9:
		// Oct 6 (Fri), Oct 5 (Thu), Oct 4 (Wed)
		expected := time.Date(2023, 10, 4, 0, 0, 0, 0, time.UTC)

		actual := getPastBusinessDay(3)

		assert.Equal(t, expected, actual)
	})

	t.Run("should return the same day for 0 days", func(t *testing.T) {
		fixedDate := time.Date(2023, 10, 11, 0, 0, 0, 0, time.UTC)
		timeNow = func() time.Time { return fixedDate }

		actual := getPastBusinessDay(0)

		assert.Equal(t, fixedDate, actual, "Should return the current date for 0 days")
	})
}
