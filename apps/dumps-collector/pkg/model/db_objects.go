package model

import (
	"time"

	"github.com/google/uuid"
)

const podActiveTimeout = time.Minute * 5

// Pods

type Pod struct {
	Id          uuid.UUID `gorm:"primaryKey;type:uuid"`
	Namespace   string
	ServiceName string
	PodName     string
	RestartTime time.Time
	LastActive  *time.Time
}

func (p *Pod) IsOnline() bool {
	if p.LastActive == nil {
		return false
	}
	return time.Since(*p.LastActive) <= podActiveTimeout
}

// Timeline

type TimelineStatus string

const (
	RawStatus      = TimelineStatus("raw")
	ZippingStatus  = TimelineStatus("zipping")
	ZippedStatus   = TimelineStatus("zipped")
	RemovingStatus = TimelineStatus("removing")
)

type Timeline struct {
	TsHour time.Time `gorm:"primaryKey"`
	Status TimelineStatus
}

// Heap dumps

type HeapDump struct {
	Handle       string    `gorm:"primaryKey"`
	PodId        uuid.UUID `gorm:"type:uuid"`
	CreationTime time.Time
	FileSize     int64
}

// Td/top dumps

type DumpType string

const (
	TdDumpType   = DumpType("td")
	TopDumpType  = DumpType("top")
	HeapDumpType = DumpType("heap")
)

func (d DumpType) GetFileSuffix() string {
	switch d {
	case TdDumpType:
		return ".td.txt"
	case TopDumpType:
		return ".top.txt"
	case HeapDumpType:
		return ".hprof.zip"
	default:
		return ""
	}
}

type DumpObject struct {
	Id           uuid.UUID `gorm:"primaryKey;type:uuid"`
	PodId        uuid.UUID `gorm:"type:uuid"`
	CreationTime time.Time
	FileSize     int64
	DumpType     DumpType
}

type StoreDumpResult struct {
	TimelinesCreated   int64
	PodsCreated        int64
	HeapDumpsInserted  int64
	TdTopDumpsInserted int64
}
