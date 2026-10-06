package model

// -----------------------------------------------------------------------------

type (
	TableStatus string
	TableType   string
)

const (
	TableStatusCreating   = TableStatus("creating")
	TableStatusReady      = TableStatus("ready")
	TableStatusPersisting = TableStatus("persisting")
	TableStatusPersisted  = TableStatus("persisted")
	TableStatusToDelete   = TableStatus("to_delete")

	TableCalls         = TableType("calls")
	TableTraces        = TableType("traces")
	TableDumps         = TableType("dumps")
	TableSuspend       = TableType("suspend")
	TableInvertedIndex = TableType("inverted_index")
)

var (
	AllTableStatuses = [...]TableStatus{TableStatusCreating, TableStatusReady, TableStatusPersisting, TableStatusPersisted, TableStatusToDelete}
	AllTableTypes    = [...]TableType{TableCalls, TableTraces, TableDumps, TableSuspend, TableInvertedIndex}
)

// -----------------------------------------------------------------------------

type (
	PodType  string
	DumpType string
)

const (
	PodTypeJava = PodType("java")
	PodTypeGo   = PodType("go")

	DumpTypeTd         = DumpType("td")
	DumpTypeTop        = DumpType("top")
	DumpTypeGc         = DumpType("gc")
	DumpTypeAlloc      = DumpType("alloc")
	DumpTypeGoroutine  = DumpType("goroutine")
	DumpTypeHeap       = DumpType("heap")
	DumpTypeProfile    = DumpType("profile")
	DumpTypeThreadInfo = DumpType("thread_info")
)

var (
	AllPodTypes  = [...]PodType{PodTypeJava, PodTypeGo}
	AllDumpTypes = [...]DumpType{DumpTypeTd, DumpTypeTop, DumpTypeGc, DumpTypeAlloc, DumpTypeGoroutine, DumpTypeHeap, DumpTypeProfile, DumpTypeThreadInfo}
)

// -----------------------------------------------------------------------------

type (
	FileType   string
	FileStatus string
)

const (
	FileCalls  = FileType("calls")
	FileTraces = FileType("traces")
	FileDumps  = FileType("dumps")
	FileHeap   = FileType("heap")

	FileCreating     = FileStatus("creating")
	FileCreated      = FileStatus("created")
	FileTransferring = FileStatus("transferring")
	FileCompleted    = FileStatus("completed")
	FileDeleted      = FileStatus("to_delete")
)

var (
	AllFileTypes    = [...]FileType{FileCalls, FileTraces, FileDumps, FileHeap}
	AllFileStatuses = [...]FileStatus{FileCreating, FileCreated, FileTransferring, FileCompleted, FileDeleted}
)
