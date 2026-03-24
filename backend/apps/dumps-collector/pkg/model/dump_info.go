package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type DumpInfo struct {
	Pod          Pod
	CreationTime time.Time
	FileSize     int64
	DumpType     DumpType
}

func (d DumpInfo) GetHandle() string {
	return fmt.Sprintf("%s-heap-%d", d.Pod.PodName, d.CreationTime.UnixMilli())
}

type DumpSummary struct {
	PodId       uuid.UUID
	DateFrom    time.Time
	DateTo      time.Time
	SumFileSize int64
}
