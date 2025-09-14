package main

import "time"

// Trade represents a single trade from the CSV file
type Trade struct {
	DataNegocio         time.Time `json:"data_negocio,omitempty"`
	CodigoInstrumento   string    `json:"codigo_instrumento,omitempty"`
	PrecoNegocio        float64   `json:"preco_negocio,omitempty"`
	QuantidadeNegociada int64     `json:"quantidade_negociada,omitempty"`
	HoraFechamento      string    `json:"hora_fechamento,omitempty"`
}
