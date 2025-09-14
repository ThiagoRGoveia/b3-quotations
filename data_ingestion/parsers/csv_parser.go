package parsers

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ThiagoRGoveia/b3-cotations.git/data-ingestion/models"
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
func ParseCSV(filePath string, fileID int, results chan<- *models.Trade) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'

	// Skip header
	if _, err := reader.Read(); err != nil {
		return err
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // Skip corrupted records // add this error to log later
		}

		trade, err := parseRecord(record, fileID)
		if err != nil {
			continue // Skip records that can't be parsed // add this error to log later
		}

		results <- trade
	}

	return nil
}
