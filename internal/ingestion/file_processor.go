package ingestion

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/database"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/models"
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/internal/parser"
)

// Processor defines the interface for file processing operations.
type Processor interface {
	ScanForFiles(rootPath string) ([]models.FileInfo, error)
	UpdateFileStatus(fileErrorsMap *models.FileErrorMap, fileMap *models.FileMap) error
}

// FileProcessor handles the initial stages of file processing, including
// discovering files, dispatching jobs, and updating file statuses.
type FileProcessor struct {
	dbManager database.DBManager
}

// NewFileProcessor creates a new FileProcessor with the given DBManager.
func NewFileProcessor(dbManager database.DBManager) *FileProcessor {
	return &FileProcessor{
		dbManager: dbManager,
	}
}

// ScanForFiles scans a directory for files, extracts a reference date from each,
// and returns a list of FileInfo structs. This initial scan helps determine
// which database partitions will be needed before full processing begins.
func (fp *FileProcessor) ScanForFiles(rootPath string) ([]models.FileInfo, error) {
	var fileInfos []models.FileInfo
	log.Printf("Scanning for files in: %s", rootPath)

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err // Propagate errors from walking the path
		}
		if !info.IsDir() {
			// Get the reference date from the file's content
			refDate, err := parser.GetReferenceDateFromFile(path)
			if err != nil {
				log.Printf("WARN: Could not get reference date from file %s: %v. Skipping file.", path, err)
				return nil // Skip this file, but continue walking
			}

			fileInfos = append(fileInfos, models.FileInfo{Path: path, ReferenceDate: refDate})
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error walking directory %s: %w", rootPath, err)
	}

	log.Printf("Found %d files to process.", len(fileInfos))
	return fileInfos, nil
}

func (fp *FileProcessor) UpdateFileStatus(fileErrorsMap *models.FileErrorMap, fileMap *models.FileMap) error {
	for fileID := range *fileMap {
		appErrors := fileErrorsMap.Errors[fileID]
		status := database.FILE_STATUS_DONE
		if len(appErrors) > 0 {
			status = database.FILE_STATUS_DONE_WITH_ERRORS
		}

		if err := fp.dbManager.UpdateFileStatus(fileID, status, appErrors); err != nil {
			log.Printf("Failed to update status for fileID %d: %v\n", fileID, err)
		}
	}
	return nil
}

func isPartitionFirstWrite(partitionName time.Time, partitionMap *models.FirstWritePartition) bool {
	wasCreatedThisRun := (*partitionMap)[partitionName]
	return wasCreatedThisRun
}
