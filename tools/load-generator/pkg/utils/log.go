package utils

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"time"
)

const (
	LogLevel   = "logLevel" // param for ctx
	Error      = "error"
	Info       = "info "
	Debug      = "debug"
	Trace      = "trace"
	ExtraTrace = "extra"
)

func IsExtraTraceEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return ctx.Value(LogLevel) == ExtraTrace // only for internal debug
}

func LogExtraTrace(ctx context.Context, format string, a ...any) {
	if IsTraceEnabled(ctx) {
		fmt.Printf(header(4, ExtraTrace, format), a...)
	}
}

func IsTraceEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return ctx.Value(LogLevel) == ExtraTrace || ctx.Value(LogLevel) == Trace
}

func LogTrace(ctx context.Context, format string, a ...any) {
	if IsTraceEnabled(ctx) {
		fmt.Printf(header(2, Trace, format), a...)
	}
}

func IsDebugEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return ctx.Value(LogLevel) == ExtraTrace || ctx.Value(LogLevel) == Trace || ctx.Value(LogLevel) == Debug
}

func LogDebug(ctx context.Context, format string, a ...any) {
	if IsDebugEnabled(ctx) {
		fmt.Printf(header(2, Debug, format), a...)
	}
}

func IsInfoEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return ctx.Value(LogLevel) == ExtraTrace || ctx.Value(LogLevel) == Trace || ctx.Value(LogLevel) == Debug || ctx.Value(LogLevel) == Info
}

func LogInfo(ctx context.Context, format string, a ...any) {
	if IsInfoEnabled(ctx) {
		fmt.Printf(header(2, Info, format), a...)
	}
}

func IsErrorEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return ctx.Value(LogLevel) == ExtraTrace || ctx.Value(LogLevel) == Trace || ctx.Value(LogLevel) == Debug || ctx.Value(LogLevel) == Info || ctx.Value(LogLevel) == Error
}

func LogError(ctx context.Context, err error, format string, a ...any) {
	if IsErrorEnabled(ctx) {
		arr := append(a, err)
		if err == nil {
			arr = a
		} else {
			format = format + ": %+v"
		}
		fmt.Printf(header(2, Error, format), arr...)
	}
}

func LogFatal(ctx context.Context, err error, format string, a ...any) {
	fmt.Printf(header(2, Error, format), a...)
	panic(err)
}

func header(skip int, level string, format string) string {
	_, filename, line, _ := runtime.Caller(skip)
	return fmt.Sprintf("[%s][%s][%s:%d] %s\n",
		time.Now().Format(time.TimeOnly), level, filepath.Base(filename), line,
		format)
}
