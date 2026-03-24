package view

import "fmt"

var (
	UndefinedTraceId = TraceTreeRowId{0, 0, 0, 0, 0, "", 0}
)

type (
	TraceTreeRowId struct {
		traceFileIndex      int
		bufferOffset        int
		recordIndex         int
		reactorFileIndex    int
		reactorBufferOffset int
		fullRowId           string
		folderId            int
	}

	TraceTree struct {
		root       Hotspot
		ganttInfos []GanttInfo
		dict       TagDictionary
		clobValues ClobValues
		ownDict    bool
		rowid      TraceTreeRowId
	}

	Hotspot struct {
		id                   int
		children             []Hotspot
		tags                 []HotspotTag
		mostImportantTags    []HotspotTag
		reactorCallId        int
		lastAssemblyId       map[uint64]bool
		lastParentAssemblyId uint64
		isReactorEndPoint    byte
		isReactorFrame       byte
		reactorDuration      int
		reactorStartTime     uint64
		reactorLeastTime     uint64
		emit                 int
		blockingOperator     int
		prevOperation        int
		currentOperation     int
		fullRowId            string
		folderId             int
		childTime            uint64
		totalTime            uint64
		childCount           int
		count                int
		suspensionTime       int
		childSuspensionTime  int
		startTime            uint64
		endTime              uint64
		//tags                 map[HotspotTag]HotspotTag
	}

	HotspotTag struct {
		id               int
		count            int
		assemblyId       uint64
		totalTime        uint64
		value            interface{}
		reactorStartDate uint64
		isParallel       byte
		parallels        []struct {
			p1 int
			p2 int
		}
	}

	ClobValues struct {
		list          []ClobValue
		observedClobs map[ClobValue]bool
	}

	ClobValue struct {
		dataFolderPath string
		folder         string
		fileIndex      int
		offset         int
		value          string
	}

	GanttInfo struct {
		id        int
		emit      int
		startTime uint64
		totalTime uint64
		fullRow   string
		folderId  int
	}

	TagDictionary struct {
		tMap      map[string]int
		methods   []string
		paramInfo map[string]interface{}
		//paramInfo map[string]ParameterInfoDto
		//ids BitSet
	}
)

func CreateTreeRowId(folderId, sequenceId, offset, record int) TraceTreeRowId {
	return TraceTreeRowId{
		traceFileIndex:      sequenceId,
		bufferOffset:        offset,
		recordIndex:         record,
		reactorFileIndex:    0,
		reactorBufferOffset: 0,
		fullRowId:           fmt.Sprintf("%d_%d_%d_%d", folderId, sequenceId, offset, record),
		folderId:            folderId,
	}
}

func (t TraceTreeRowId) Compare(o TraceTreeRowId) int {
	if t.traceFileIndex != o.traceFileIndex {
		if t.traceFileIndex < o.traceFileIndex {
			return -1
		} else {
			return 1
		}
	}

	if t.bufferOffset != o.bufferOffset {
		if t.bufferOffset < o.bufferOffset {
			return -1
		} else {
			return 1
		}
	}

	if t.recordIndex != o.recordIndex {
		if t.recordIndex < o.recordIndex {
			return -1
		} else {
			return 1
		}
	}

	return 0
}

func (t TraceTreeRowId) String() string {
	return fmt.Sprintf("TreeRowid{traceFileIndex=%v, bufferOffset=%v, recordIndex=%v, "+
		"reactorFileIndex=%v, reactorBufferOffset=%v, fullRowId=%v, folderId=%v}",
		t.traceFileIndex, t.bufferOffset, t.recordIndex,
		t.reactorFileIndex, t.reactorBufferOffset, t.fullRowId, t.folderId)
}
