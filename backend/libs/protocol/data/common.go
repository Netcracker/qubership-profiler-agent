package data

type (
	LTime     = int64  // Unix time (milliseconds, UTC)
	LDuration = int    // Duration (milliseconds)
	LCounter  = int    // Counter (calls, transactions, etc.)
	LBytes    = uint64 // Long counter (bytes, etc.)
	TagId     = int    // tag id (from dictionary)
)
