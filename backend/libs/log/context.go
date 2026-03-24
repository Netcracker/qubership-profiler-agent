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
