package models

import "time"

// FileInfo holds the path and reference date for a file.
type FileInfo struct {
	Path          string
	ReferenceDate time.Time
}
