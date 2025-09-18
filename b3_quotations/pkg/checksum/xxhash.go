package checksum

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cespare/xxhash/v2"
)

func GetFileChecksum(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	hasher := xxhash.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("failed to copy file content to hasher for file %s: %w", filePath, err)
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func CalculateHash(record []string) string {
	lineContent := strings.Join(record, ";")

	digest := xxhash.New()
	digest.Write([]byte(lineContent))

	return hex.EncodeToString(digest.Sum(nil))
}
