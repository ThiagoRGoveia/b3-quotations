package models

import "fmt"

// AppError represents a custom error in the application, including context about the job.
type AppError struct {
	FileID  int
	Message string
	Err     error
}

// Error returns the string representation of the AppError.
func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("FileID %d: %s - %v", e.FileID, e.Message, e.Err)
	}
	return fmt.Sprintf("FileID %d: %s", e.FileID, e.Message)
}
