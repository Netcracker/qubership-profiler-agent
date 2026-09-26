package log

import (
	"context"
	"fmt"
	"strings"
)

type (
	level string
)

const (
	ERROR   = level("ERROR")
	WARNING = level("WARNING")
	INFO    = level("INFO")
	DEBUG   = level("DEBUG")
	TRACE   = level("TRACE")
	EXTRA   = level("extra")
)

var (
	defaultLevel = INFO
)

func mapStringToLevel(logLevel string) (level, error) {
	switch strings.ToLower(logLevel) {
	case "trace":
		return TRACE, nil
	case "debug":
		return DEBUG, nil
	case "info":
		return INFO, nil
	case "warning":
		return WARNING, nil
	case "warn":
		return WARNING, nil
	case "error":
		return ERROR, nil
	case "off":
		return ERROR, nil
	case "":
		return defaultLevel, nil
	default:
		return defaultLevel, fmt.Errorf("unknown log level: %s", logLevel)
	}
}

// GetLevel returns the level stored in ctx by [SetLevel], or the default level
// [INFO] when ctx is nil or carries no recognized level.
func GetLevel(ctx context.Context) level {
	if ctx == nil {
		return defaultLevel
	}

	switch ctx.Value(LevelKey) {
	case ERROR:
		return ERROR
	case WARNING:
		return WARNING
	case INFO:
		return INFO
	case DEBUG:
		return DEBUG
	case TRACE:
		return TRACE
	case EXTRA:
		return EXTRA
	default:
		return defaultLevel
	}
}

// verbosity ranks l so that a more verbose level ranks higher; an unknown level
// ranks 0.
func verbosity(l level) int {
	switch l {
	case ERROR:
		return 1
	case WARNING:
		return 2
	case INFO:
		return 3
	case DEBUG:
		return 4
	case TRACE:
		return 5
	case EXTRA:
		return 6
	default:
		return 0
	}
}

// enabled reports whether ctx prints a record at level l, which holds when the
// level [GetLevel] returns for ctx is at least as verbose as l. A context with
// no level therefore prints records at the default level, [INFO], and below.
func enabled(ctx context.Context, l level) bool {
	return verbosity(GetLevel(ctx)) >= verbosity(l)
}
