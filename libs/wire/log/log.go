package log

import (
	"context"
	"fmt"
)

func Fatal(ctx context.Context, err error, format string, a ...any) {
	fmt.Printf(header(ctx, 2, ERROR, format), a...)
	panic(err)
}

func IsErrorEnabled(ctx context.Context) bool {
	if ctx == nil {
		return true
	}
	return ctx.Value(LevelKey) == EXTRA || ctx.Value(LevelKey) == TRACE || ctx.Value(LevelKey) == DEBUG || ctx.Value(LevelKey) == INFO || ctx.Value(LevelKey) == ERROR || ctx.Value(LevelKey) == nil
}

func Error(ctx context.Context, err error, format string, a ...any) {
	if IsErrorEnabled(ctx) {
		arr := append(a, err)
		if err == nil {
			arr = a
		} else {
			format = format + ": %+v"
		}
		fmt.Printf(header(ctx, 2, ERROR, format), arr...)
	}
}

func ErrorWithoutCtx(format string, a ...any) {
	Error(context.Background(), nil, format, a...)
}

func IsWarningEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return ctx.Value(LevelKey) == EXTRA || ctx.Value(LevelKey) == TRACE || ctx.Value(LevelKey) == DEBUG || ctx.Value(LevelKey) == INFO || ctx.Value(LevelKey) == WARNING
}

func Warning(ctx context.Context, format string, a ...any) {
	if IsWarningEnabled(ctx) {
		fmt.Printf(header(ctx, 2, WARNING, format), a...)
	}
}

func IsInfoEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return ctx.Value(LevelKey) == EXTRA || ctx.Value(LevelKey) == TRACE || ctx.Value(LevelKey) == DEBUG || ctx.Value(LevelKey) == INFO
}

func Info(ctx context.Context, format string, a ...any) {
	if IsInfoEnabled(ctx) {
		fmt.Printf(header(ctx, 2, INFO, format), a...)
	}
}

func IsDebugEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return ctx.Value(LevelKey) == EXTRA || ctx.Value(LevelKey) == TRACE || ctx.Value(LevelKey) == DEBUG
}

func Debug(ctx context.Context, format string, a ...any) {
	if IsDebugEnabled(ctx) {
		fmt.Printf(header(ctx, 2, DEBUG, format), a...)
	}
}

func IsTraceEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return ctx.Value(LevelKey) == EXTRA || ctx.Value(LevelKey) == TRACE
}

func Trace(ctx context.Context, format string, a ...any) {
	if IsTraceEnabled(ctx) {
		fmt.Printf(header(ctx, 2, TRACE, format), a...)
	}
}

func IsExtraTraceEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	return ctx.Value(LevelKey) == EXTRA // only for internal debug
}

func ExtraTrace(ctx context.Context, format string, a ...any) {
	if IsExtraTraceEnabled(ctx) {
		fmt.Printf(header(ctx, 4, EXTRA, format), a...)
	}
}
