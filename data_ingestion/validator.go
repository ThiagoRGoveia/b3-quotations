package main

import (
	"fmt"
)

// requiredFields specifies the indices of fields that cannot be empty.
var requiredFields = map[int]string{
	1: "CodigoInstrumento",
	3: "PrecoNegocio",
	4: "QuantidadeNegociada",
	5: "HoraFechamento",
	8: "DataNegocio",
}

// validateRecord checks if all required fields in a CSV record are non-empty.
func validateRecord(record []string) error {
	for index, name := range requiredFields {
		if index >= len(record) || record[index] == "" {
			return fmt.Errorf("validation failed: field %s is null or empty", name)
		}
	}
	return nil
}
