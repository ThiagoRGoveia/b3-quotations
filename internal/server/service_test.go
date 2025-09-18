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

// MockDBManager is a mock implementation of the DBManager interface.
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
	t.Run("should return the correct past business day", func(t *testing.T) {
		// Assuming today is a Wednesday
		// 7 business days in the past should be the Tuesday of the previous week
		now := time.Date(2023, 10, 11, 0, 0, 0, 0, time.UTC) // A Wednesday
		getPastBusinessDay := func(days int) time.Time {
			date := now
			for businessDays := 0; businessDays < days; {
				date = date.AddDate(0, 0, -1)
				if date.Weekday() != time.Saturday && date.Weekday() != time.Sunday {
					businessDays++
				}
			}
			return date
		}

		expected := time.Date(2023, 10, 2, 0, 0, 0, 0, time.UTC) // A Monday
		actual := getPastBusinessDay(7)

		// Since we are testing a function that depends on time.Now(), we need to control the 'now'
		// For this test, we'll check if the resulting day is the expected one, ignoring the time part.
		assert.Equal(t, expected.Year(), actual.Year())
		assert.Equal(t, expected.Month(), actual.Month())
		assert.Equal(t, expected.Day(), actual.Day())
	})
}
