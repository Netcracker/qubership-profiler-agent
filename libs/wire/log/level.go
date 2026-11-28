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

func GetLevel(ctx context.Context) level {
	if ctx == nil {
		return INFO
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
