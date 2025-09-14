package models

// FileJob represents a file to be processed, including its path and database ID.
type FileJob struct {
	FilePath string
	FileID   int
}
