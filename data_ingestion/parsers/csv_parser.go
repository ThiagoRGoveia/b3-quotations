package parsers

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/models"
	"github.com/cespare/xxhash/v2"
)

// GetReferenceDateFromFile opens a CSV file, reads the first data row, and returns the reference date.
func GetReferenceDateFromFile(filePath string) (time.Time, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'

	// Skip header
	if _, err := reader.Read(); err != nil {
		return time.Time{}, fmt.Errorf("failed to read header from %s: %w", filePath, err)
	}

	// Read the first data record
	record, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return time.Time{}, fmt.Errorf("file %s is empty or has only a header", filePath)
		}
		return time.Time{}, fmt.Errorf("failed to read first data record from %s: %w", filePath, err)
	}

	// The reference date is in the first column
	if len(record) == 0 {
		return time.Time{}, fmt.Errorf("empty record in file %s", filePath)
	}

	dataReferencia, err := time.Parse("2006-01-02", record[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse reference date from %s in file %s: %w", record[0], filePath, err)
	}

	return dataReferencia, nil
}

// parseRecord parses a single CSV record into a Trade struct.
func parseRecord(record []string, fileID int) (*models.Trade, error) {
	// DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;QuantidadeNegociada;HoraFechamento;CodigoIdentificadorNegocio;TipoSessaoPregao;DataNegocio;CodigoParticipanteComprador;CodigoParticipanteVendedor
	precoNegocioStr := strings.Replace(record[3], ",", ".", 1)
	precoNegocio, err := strconv.ParseFloat(precoNegocioStr, 64)
	if err != nil {
		return nil, err
	}

	quantidadeNegociada, err := strconv.ParseInt(record[4], 10, 64)
	if err != nil {
		return nil, err
	}

	if record[8] == "" {
		return nil, fmt.Errorf("empty date")
	}

	dataReferencia, err := time.Parse("2006-01-02", record[0])
	if err != nil {
		return nil, err
	}

	dataNegocio, err := time.Parse("2006-01-02", record[8])
	if err != nil {
		return nil, err
	}

	codigoIdentificadorNegocio := record[6]

	return &models.Trade{
		FileID:          fileID,
		ReferenceDate:   dataReferencia,
		Ticker:          record[1],
		Price:           precoNegocio,
		Quantity:        quantidadeNegociada,
		ClosingTime:     record[5],
		TransactionDate: dataNegocio,
		Identifier:      codigoIdentificadorNegocio,
	}, nil
}

// ParseCSV reads a CSV file from the given path and streams parsed records into a channel.
func ParseCSV(filePath string, fileID int, results chan<- *models.Trade, errors chan<- models.AppError) error {
	file, err := os.Open(filePath)
	if err != nil {
		errors <- models.AppError{FileID: fileID, Message: "Failed to open file", Err: err}
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'

	// Skip header
	if _, err := reader.Read(); err != nil {
		errors <- models.AppError{FileID: fileID, Message: "Failed to read header from CSV", Err: err}
		return err
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errors <- models.AppError{FileID: fileID, Message: "Failed to read record from CSV", Err: err}
			continue // Skip corrupted records
		}

		// Calculate MD5 hash of the raw CSV line
		lineHash := calculateHash(record)

		trade, err := parseRecord(record, fileID)
		if err != nil {
			errors <- models.AppError{FileID: fileID, Message: "Failed to parse record", Err: err}
			continue // Skip records that can't be parsed
		}

		// Set the hash in the trade record
		trade.Hash = lineHash

		// Validate trade record
		if trade.IsValid() {
			results <- trade
		} else {
			errors <- models.AppError{FileID: fileID, Message: "Invalid trade record", Err: err, Trade: trade}
		}
	}

	return nil
}

// calculateHash generates an MD5 hash for a CSV record
func calculateHash(record []string) string {
	// Use strings.Join for efficient concatenation.
	lineContent := strings.Join(record, ";")

	// Create xxHash hash
	digest := xxhash.New()
	digest.Write([]byte(lineContent))

	// Convert to hex string
	return hex.EncodeToString(digest.Sum(nil))
}
