package ingestion

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/database"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const csvHeader = "DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;QuantidadeNegociada;HoraFechamento;CodigoIdentificadorNegocio;TipoSessaoPregao;DataNegocio;CodigoParticipanteComprador;CodigoParticipanteVendedor"

type CSVRow struct {
	DataReferencia              string
	CodigoInstrumento           string
	AcaoAtualizacao             string
	PrecoNegocio                string
	QuantidadeNegociada         string
	HoraFechamento              string
	CodigoIdentificadorNegocio  string
	TipoSessaoPregao            string
	DataNegocio                 string
	CodigoParticipanteComprador string
	CodigoParticipanteVendedor  string
}

func newDefaultCSVRow() CSVRow {
	return CSVRow{
		DataReferencia:              "2025-09-08",
		CodigoInstrumento:           "ABS123",
		AcaoAtualizacao:             "0",
		PrecoNegocio:                "4310,000",
		QuantidadeNegociada:         "1",
		HoraFechamento:              "090000006",
		CodigoIdentificadorNegocio:  "10",
		TipoSessaoPregao:            "1",
		DataNegocio:                 "2025-09-08",
		CodigoParticipanteComprador: "120",
		CodigoParticipanteVendedor:  "4090",
	}
}

func createTestCSVContent(rows []CSVRow) string {
	var content strings.Builder
	content.WriteString(csvHeader + "\n")

	for _, rowData := range rows {
		row := []string{
			rowData.DataReferencia,
			rowData.CodigoInstrumento,
			rowData.AcaoAtualizacao,
			rowData.PrecoNegocio,
			rowData.QuantidadeNegociada,
			rowData.HoraFechamento,
			rowData.CodigoIdentificadorNegocio,
			rowData.TipoSessaoPregao,
			rowData.DataNegocio,
			rowData.CodigoParticipanteComprador,
			rowData.CodigoParticipanteVendedor,
		}
		content.WriteString(fmt.Sprintf("%s\n", strings.Join(row, ";")))
	}

	return content.String()
}

// MockDBManager is a mock implementation of the DBManager interface for testing.
type FileProcessorMockDBManager struct {
	mock.Mock
}

func (m *FileProcessorMockDBManager) UpdateFileStatus(fileID int, status string, errors any) error {
	args := m.Called(fileID, status, errors)
	return args.Error(0)
}

// TestFileProcessor_ScanForFiles tests the ScanForFiles method of FileProcessor.
func TestFileProcessor_ScanForFiles(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "scan_test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create dummy files for testing
	date1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	file1Path := filepath.Join(tempDir, "file1.txt")
	row1 := newDefaultCSVRow()
	row1.DataReferencia = date1.Format("2006-01-02")
	file1Content := createTestCSVContent([]CSVRow{row1})
	assert.NoError(t, os.WriteFile(file1Path, []byte(file1Content), 0644))

	date2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	file2Path := filepath.Join(tempDir, "file2.txt")
	row2 := newDefaultCSVRow()
	row2.DataReferencia = date2.Format("2006-01-02")
	file2Content := createTestCSVContent([]CSVRow{row2})
	assert.NoError(t, os.WriteFile(file2Path, []byte(file2Content), 0644))

	// Create a file with invalid date format
	file3Path := filepath.Join(tempDir, "file3.txt")
	assert.NoError(t, os.WriteFile(file3Path, []byte("invalid-date"), 0644))

	dbManager := new(MockDBManager)
	fileProcessor := NewFileProcessor(dbManager)

	t.Run("Success", func(t *testing.T) {
		fileInfos, err := fileProcessor.ScanForFiles(tempDir)

		assert.NoError(t, err)
		assert.Len(t, fileInfos, 2)

		// Check file info details
		found1 := false
		found2 := false
		found3 := false
		for _, info := range fileInfos {
			if info.Path == file1Path {
				assert.Equal(t, date1, info.ReferenceDate)
				found1 = true
			}
			if info.Path == file2Path {
				assert.Equal(t, date2, info.ReferenceDate)
				found2 = true
			}
			if info.Path == file3Path {
				found3 = true
			}
		}
		assert.True(t, found1, "file1.txt not found in scan results")
		assert.True(t, found2, "file2.txt not found in scan results")
		assert.False(t, found3, "file3.txt should not be found in scan results")
	})

	t.Run("DirectoryNotFound", func(t *testing.T) {
		_, err := fileProcessor.ScanForFiles("non_existent_dir")
		assert.Error(t, err)
	})
}

// TestFileProcessor_UpdateFileStatus tests the UpdateFileStatus method of FileProcessor.
func TestFileProcessor_UpdateFileStatus(t *testing.T) {
	dbManager := new(MockDBManager)
	fileProcessor := NewFileProcessor(dbManager)

	t.Run("StatusDone", func(t *testing.T) {
		fileMap := models.FileMap{1: "file1.txt"}
		fileErrorsMap := models.FileErrorMap{Errors: make(map[int][]models.AppError)}

		dbManager.On("UpdateFileStatus", 1, database.FILE_STATUS_DONE, mock.Anything).Return(nil).Once()

		err := fileProcessor.UpdateFileStatus(&fileErrorsMap, &fileMap)

		assert.NoError(t, err)
		dbManager.AssertExpectations(t)
	})

	t.Run("StatusDoneWithErrors", func(t *testing.T) {
		fileMap := models.FileMap{1: "file1.txt"}
		appErrors := []models.AppError{{Message: "some error"}}
		fileErrorsMap := models.FileErrorMap{Errors: map[int][]models.AppError{1: appErrors}}

		dbManager.On("UpdateFileStatus", 1, database.FILE_STATUS_DONE_WITH_ERRORS, appErrors).Return(nil).Once()

		err := fileProcessor.UpdateFileStatus(&fileErrorsMap, &fileMap)

		assert.NoError(t, err)
		dbManager.AssertExpectations(t)
	})

	t.Run("UpdateError", func(t *testing.T) {
		fileMap := models.FileMap{1: "file1.txt"}
		fileErrorsMap := models.FileErrorMap{Errors: make(map[int][]models.AppError)}
		updateErr := fmt.Errorf("db update failed")

		dbManager.On("UpdateFileStatus", 1, database.FILE_STATUS_DONE, mock.Anything).Return(updateErr).Once()

		err := fileProcessor.UpdateFileStatus(&fileErrorsMap, &fileMap)

		assert.NoError(t, err)
		dbManager.AssertExpectations(t)
	})
}
