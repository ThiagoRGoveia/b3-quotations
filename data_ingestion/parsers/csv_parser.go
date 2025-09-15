package parsers

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/data-ingestion/models"
)

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

	dataNegocio, err := time.Parse("2006-01-02", record[8])
	if err != nil {
		return nil, err
	}

	return &models.Trade{
		FileID:              fileID,
		CodigoInstrumento:   record[1],
		PrecoNegocio:        precoNegocio,
		QuantidadeNegociada: quantidadeNegociada,
		HoraFechamento:      record[5],
		DataNegocio:         dataNegocio,
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

		trade, err := parseRecord(record, fileID)
		if err != nil {
			errors <- models.AppError{FileID: fileID, Message: "Failed to parse record", Err: err}
			continue // Skip records that can't be parsed
		}

		results <- trade
	}

	return nil
}
