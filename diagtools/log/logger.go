package log

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

type contextKey string

const (
	ContextKey contextKey = "context"
)

var (
	logger *slog.Logger
)

func ChildCtx(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, ContextKey, name)
}

// AppendCtx append subcontext name to existing
func AppendCtx(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, ContextKey, fmt.Sprintf("%s:%s", contextName(ctx), name))
}

func GetLogLevel(logLevelEnv string) (level slog.Level) {
	logLevelEnv = strings.ToLower(logLevelEnv)
	if logLevelEnv == "trace" {
		logLevelEnv = "debug"
	}

	logLevel := flag.String("log.level", logLevelEnv, "Minimum enabled logging level: info, debug, warn, error.")

	err := level.UnmarshalText([]byte(*logLevel))
	if err != nil {
		level = slog.LevelWarn
	}
	return
}

func SetupLogger(log *slog.Logger) {
	logger = log
}

func SetupTestLogger() {
	opts := &slog.HandlerOptions{
		Level: GetLogLevel("debug"),
	}
	handler := NewHandler(os.Stdout, opts)
	SetupLogger(slog.New(handler))
}

func CreateLogger(logLevelEnv string, logToConsole bool, logPath string, logFileSize, logFileBackups, logInterval int) *slog.Logger {
	writers := []io.Writer{
		&lumberjack.Logger{
			Filename:   logPath,
			MaxSize:    logFileSize,    // megabytes
			MaxBackups: logFileBackups, // number of backups
			MaxAge:     logInterval,    // days
		},
	}

	if logToConsole && strings.ToLower(logLevelEnv) != "off" {
		writers = append(writers, os.Stdout)
	}
	mw := io.MultiWriter(writers...)

	opts := &slog.HandlerOptions{
		Level: GetLogLevel(logLevelEnv),
	}

	handler := NewHandler(mw, opts)
	return slog.New(handler)
}

func Log(ctx context.Context) (log *slog.Logger) {
	_, filename, line, ok := runtime.Caller(2)
	if !ok {
		filename = "???"
		line = 0
	}
	source := fmt.Sprintf("%s:%d", filepath.Base(filename), line)

	return logger.With(
		slog.String("source", source),
		slog.String("context", contextName(ctx)),
	)
}

func contextName(ctx context.Context) string {
	if ctx != nil {
		if val, ok := ctx.Value(ContextKey).(string); ok {
			return val
		}
	}
	return "-"
}

func Fatalf(ctx context.Context, err error, msg string, args ...any) {
	Errorf(ctx, err, msg, args...)
	os.Exit(1)
}

func Errorf(ctx context.Context, err error, msg string, args ...any) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	Log(ctx).ErrorContext(ctx, fmt.Sprintf(msg, args...), "err", errStr)
}

func Error(ctx context.Context, err error, msg string, args ...any) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	args = append([]any{"err", errStr}, args...)
	Log(ctx).ErrorContext(ctx, msg, args...)
}

func IsInfoEnabled(ctx context.Context) bool {
	return logger.Enabled(ctx, slog.LevelInfo)
}

func Infof(ctx context.Context, msg string, args ...any) {
	Log(ctx).InfoContext(ctx, fmt.Sprintf(msg, args...))
}

func Info(ctx context.Context, msg string, args ...any) {
	Log(ctx).InfoContext(ctx, msg, args...)
}

func IsDebugEnabled(ctx context.Context) bool {
	return logger.Enabled(ctx, slog.LevelDebug)
}

func Debugf(ctx context.Context, msg string, args ...any) {
	Log(ctx).DebugContext(ctx, fmt.Sprintf(msg, args...))
}

func Debug(ctx context.Context, msg string, args ...any) {
	Log(ctx).DebugContext(ctx, msg, args...)
}
