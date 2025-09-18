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
	"github.com/ThiagoRGoveia/b3-quotations.git/b3_quotations/pkg/checksum"
)

// Processor defines the interface for file processing operations.
type Processor interface {
	ScanForFiles(rootPath string) ([]models.FileInfo, error)
	PreprocessAndDispatchJobs(fileInfos []models.FileInfo, fileMap map[int]string, jobsChan chan<- models.FileProcessingJob)
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

// PreprocessAndDispatchJobs iterates through a list of discovered files, performs preliminary
// checks, creates a database record for tracking, and sends a job to the jobs channel
// for the parser workers. It handles checksum verification to ensure idempotency.
// This function should be run in a goroutine.
func (fp *FileProcessor) PreprocessAndDispatchJobs(
	fileInfos []models.FileInfo,
	fileMap map[int]string,
	jobsChan chan<- models.FileProcessingJob,
) {
	// After all files have been preprocessed and dispatched, close the jobs channel
	// to signal to the parser workers that no more work is coming.
	defer close(jobsChan)

	for _, fileInfo := range fileInfos {
		// Calculate file checksum for idempotency check
		checksum, err := checksum.GetFileChecksum(fileInfo.Path)
		if err != nil {
			log.Printf("ERROR: Failed to calculate checksum for %s: %v. Skipping file.", fileInfo.Path, err)
			continue
		}

		// Check if a file with this checksum has already been successfully processed
		isProcessed, err := fp.dbManager.IsFileAlreadyProcessed(checksum)
		if err != nil {
			log.Printf("ERROR: Failed to check if file %s is already processed: %v. Skipping file.", fileInfo.Path, err)
			continue
		}
		if isProcessed {
			log.Printf("INFO: File %s (checksum: %s) has already been processed. Skipping.", fileInfo.Path, checksum)
			continue
		}

		// Create a record in the database to track this file's processing.
		// The status is initially set to "PROCESSING".
		fileID, err := fp.dbManager.InsertFileRecord(
			fileInfo.Path,
			time.Now(),
			database.FILE_STATUS_PROCESSING,
			checksum,
			fileInfo.ReferenceDate,
		)
		if err != nil {
			log.Printf("ERROR: Failed to insert file record for %s: %v. Skipping file.", fileInfo.Path, err)
			continue
		}

		// Store the mapping of fileID to filePath for later status updates.
		fileMap[fileID] = fileInfo.Path

		// Send the job to the parser workers.
		log.Printf("Dispatching job for file: %s (FileID: %d)", fileInfo.Path, fileID)
		jobsChan <- models.FileProcessingJob{FilePath: fileInfo.Path, FileID: fileID}
	}
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
