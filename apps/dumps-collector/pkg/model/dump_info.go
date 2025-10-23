package model

import (
	"time"

	"github.com/google/uuid"
)

type DumpInfo struct {
	Pod          Pod
	CreationTime time.Time
	FileSize     int64
	DumpType     DumpType
}

type DumpSummary struct {
	PodId       uuid.UUID
	DateFrom    time.Time
	DateTo      time.Time
	SumFileSize int64
}
