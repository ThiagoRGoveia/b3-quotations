package models

// FileProcessingJob represents a file to be processed, including its path and database ID.
type FileProcessingJob struct {
	FilePath string
	FileID   int
}
