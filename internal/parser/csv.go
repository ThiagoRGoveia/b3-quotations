package parser

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/pkg/checksum"
)

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

	record, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return time.Time{}, fmt.Errorf("file %s is empty or has only a header", filePath)
		}
		return time.Time{}, fmt.Errorf("failed to read first data record from %s: %w", filePath, err)
	}

	if len(record) == 0 {
		return time.Time{}, fmt.Errorf("empty record in file %s", filePath)
	}

	dataReferencia, err := time.Parse("2006-01-02", record[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse reference date from %s in file %s: %w", record[0], filePath, err)
	}

	return dataReferencia, nil
}

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

func ParseCSV(filePath string, fileID int, dateChannels map[time.Time]chan *models.Trade, errors chan<- models.AppError) error {
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

		// we create a line checksum here to be used later for idempotency checks row by row
		lineCheckSum := checksum.CalculateCheckSum(record)

		trade, err := parseRecord(record, fileID)
		if err != nil {
			errors <- models.AppError{FileID: fileID, Message: "Failed to parse record", Err: err}
			continue // Skip records that can't be parsed
		}

		trade.CheckSum = lineCheckSum

		resultsChan, exists := dateChannels[trade.ReferenceDate.Truncate(24*time.Hour)]
		if !exists {
			errors <- models.AppError{FileID: fileID, Message: "No channel found for reference date", Err: fmt.Errorf("missing channel for date: %v", trade.ReferenceDate), Trade: trade}
			log.Printf("No channel found for reference date: %v, File ID: %d may be malformed", trade.ReferenceDate, fileID)
			continue
		}

		if trade.IsValid() {
			resultsChan <- trade
		} else {
			errors <- models.AppError{FileID: fileID, Message: "Invalid trade record", Err: err, Trade: trade}
		}
	}

	return nil
}
