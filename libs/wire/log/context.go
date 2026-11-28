package log

import (
	"context"
)

type (
	ctxKeyType string
)

const (
	LevelKey   = ctxKeyType("logLevel")   // log level  for ctx
	ContextKey = ctxKeyType("ctxKey")     // context name  for ctx
	RequestId  = ctxKeyType("request_id") // requestId for ctx
)

func SetLevelString(baseCtx context.Context, level string) (context.Context, error) {
	logLevel, err := mapStringToLevel(level)
	if err != nil {
		logLevel = defaultLevel
	}

	return SetLevel(baseCtx, logLevel), err
}

func SetLevel(baseCtx context.Context, level level) context.Context {
	return context.WithValue(baseCtx, LevelKey, level)
}

func WithName(baseCtx context.Context, contextName string) context.Context {
	return context.WithValue(baseCtx, ContextKey, contextName)
}

func Context(contextName string) context.Context {
	return WithName(context.Background(), contextName)
}

func GetContextName(ctx context.Context) string {
	if v, ok := ctx.Value(ContextKey).(string); ok {
		return v
	}
	return ""
}

type sinkKey struct{}

// Sink receives a log record in place of stdout: the level name, such as
// "INFO", and the formatted message without the header. It runs
// synchronously on the goroutine that logs, and one context can be shared by
// many goroutines, so a Sink must be safe for concurrent calls.
type Sink func(level, msg string)

// WithSink returns a context whose log records go to sink instead of stdout.
// The level in the context still decides which records are written.
func WithSink(baseCtx context.Context, sink Sink) context.Context {
	return context.WithValue(baseCtx, sinkKey{}, sink)
}

func sinkFrom(ctx context.Context) Sink {
	if ctx == nil {
		return nil
	}
	sink, _ := ctx.Value(sinkKey{}).(Sink)
	return sink
}
