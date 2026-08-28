package log

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	baseCtx = context.Background()
)

func init() {
	isTest = true
}

func TestLevels(t *testing.T) {
	assert.Equalf(t, INFO, GetLevel(baseCtx), "invalid default level")

	ctx := SetLevel(baseCtx, DEBUG)
	assert.Equalf(t, DEBUG, GetLevel(ctx), "invalid level")

	ctx = SetLevel(baseCtx, EXTRA)
	assert.Equalf(t, EXTRA, GetLevel(ctx), "invalid level")

	ctx, err := SetLevelString(baseCtx, "ERROR")
	assert.Nil(t, err)
	assert.Equalf(t, ERROR, GetLevel(ctx), "invalid level")

	ctx, err = SetLevelString(baseCtx, "off")
	assert.Nil(t, err)
	assert.Equalf(t, ERROR, GetLevel(ctx), "invalid level")

	ctx, err = SetLevelString(baseCtx, "warnING")
	assert.Nil(t, err)
	assert.Equalf(t, WARNING, GetLevel(ctx), "invalid level")

	ctx, err = SetLevelString(baseCtx, "WARN")
	assert.Nil(t, err)
	assert.Equalf(t, WARNING, GetLevel(ctx), "invalid level")

	ctx, err = SetLevelString(baseCtx, "info")
	assert.Nil(t, err)
	assert.Equalf(t, INFO, GetLevel(ctx), "invalid level")

	ctx, err = SetLevelString(baseCtx, "")
	assert.Nil(t, err)
	assert.Equalf(t, INFO, GetLevel(ctx), "invalid level")

	ctx, err = SetLevelString(baseCtx, "debuG")
	assert.Nil(t, err)
	assert.Equalf(t, DEBUG, GetLevel(ctx), "invalid level")

	ctx, err = SetLevelString(baseCtx, "trace")
	assert.Nil(t, err)
	assert.Equalf(t, TRACE, GetLevel(ctx), "invalid level")

	ctx, err = SetLevelString(baseCtx, "extra")
	assert.Errorf(t, err, "unknown log level: extra")
	assert.Equalf(t, INFO, GetLevel(ctx), "invalid level")
}

func TestError(t *testing.T) {
	assert.True(t, IsErrorEnabled(baseCtx))
	s := CaptureAsString(func() {
		Error(baseCtx, nil, "asdasdas")
	})
	assert.Equalf(t, "[2006-01-02T01:02:03.004] [ERROR] [request_id=-] [tenant_id=--] [thread=-] [class=log/log_test.go:12] asdasdas\n", s, "should not be error messages")

	ctx := SetLevel(baseCtx, ERROR)
	s = CaptureAsString(func() {
		Error(ctx, nil, "asdasdas")
	})
	assert.Equalf(t, "[2006-01-02T01:02:03.004] [ERROR] [request_id=-] [tenant_id=--] [thread=-] [class=log/log_test.go:12] asdasdas\n", s, "invalid messages")

	err := fmt.Errorf("test error")
	s = CaptureAsString(func() {
		Error(ctx, err, "asdasdas")
	})
	assert.Equalf(t, "[2006-01-02T01:02:03.004] [ERROR] [request_id=-] [tenant_id=--] [thread=-] [class=log/log_test.go:12] asdasdas: test error\n", s, "invalid messages")

	s = CaptureAsString(func() {
		ErrorWithoutCtx("error message")
	})
	assert.Equalf(t, "[2006-01-02T01:02:03.004] [ERROR] [request_id=-] [tenant_id=--] [thread=-] [class=log/log.go:12] error message\n", s, "invalid messages")

	s = CaptureAsString(func() {
		Error(nil, nil, "asdasdas")
	})
	assert.Equalf(t, "[2006-01-02T01:02:03.004] [ERROR] [request_id=-] [tenant_id=--] [thread=-] [class=log/log_test.go:12] asdasdas\n", s, "invalid messages")
	assert.True(t, IsErrorEnabled(nil))
}

func TestWarning(t *testing.T) {
	assert.False(t, IsWarningEnabled(baseCtx))
	s := CaptureAsString(func() {
		Warning(baseCtx, "asdasdas")
	})
	assert.Equalf(t, "", s, "should not be error messages")

	ctx := SetLevel(baseCtx, DEBUG)
	assert.True(t, IsWarningEnabled(ctx))
	s = CaptureAsString(func() {
		Warning(ctx, "asdasdas got %d words with prefix %s ", 123, "data")
	})
	assert.Equalf(t, "[2006-01-02T01:02:03.004] [WARNING] [request_id=-] [tenant_id=--] [thread=-] [class=log/log_test.go:12] asdasdas got 123 words with prefix data \n", s, "invalid messages")

	ctx = SetLevel(baseCtx, WARNING)
	assert.True(t, IsWarningEnabled(ctx))
	s = CaptureAsString(func() {
		Warning(ctx, "asdasdas")
	})
	assert.Equalf(t, "[2006-01-02T01:02:03.004] [WARNING] [request_id=-] [tenant_id=--] [thread=-] [class=log/log_test.go:12] asdasdas\n", s, "invalid messages")

	ctx = SetLevel(baseCtx, ERROR)
	assert.False(t, IsWarningEnabled(ctx))
	s = CaptureAsString(func() {
		Warning(ctx, "asdasdas")
	})
	assert.Equalf(t, "", s, "invalid messages")
	assert.False(t, IsWarningEnabled(nil))
}

func TestInfo(t *testing.T) {
	assert.False(t, IsInfoEnabled(baseCtx))
	s := CaptureAsString(func() {
		Info(baseCtx, "asdasdas")
	})
	assert.Equalf(t, "", s, "should not be error messages")

	ctx := SetLevel(baseCtx, DEBUG)
	s = CaptureAsString(func() {
		Info(ctx, "asdasdas got %d words with prefix %s ", 123, "data")
	})
	assert.Equalf(t, "[2006-01-02T01:02:03.004] [INFO] [request_id=-] [tenant_id=--] [thread=-] [class=log/log_test.go:12] asdasdas got 123 words with prefix data \n", s, "invalid messages")

	ctx = SetLevel(baseCtx, WARNING)
	s = CaptureAsString(func() {
		Info(ctx, "asdasdas")
	})
	assert.Equalf(t, "", s, "invalid messages")
	assert.False(t, IsInfoEnabled(nil))
}

func TestDebug(t *testing.T) {
	assert.False(t, IsDebugEnabled(baseCtx))
	s := CaptureAsString(func() {
		Debug(baseCtx, "asdasdas")
	})
	assert.Equalf(t, "", s, "should not be error messages")

	ctx := SetLevel(baseCtx, DEBUG)
	s = CaptureAsString(func() {
		Debug(ctx, "asdasdas got %d words with prefix %s ", 123, "data")
	})
	assert.Equalf(t, "[2006-01-02T01:02:03.004] [DEBUG] [request_id=-] [tenant_id=--] [thread=-] [class=log/log_test.go:12] asdasdas got 123 words with prefix data \n", s, "invalid messages")

	ctx = SetLevel(baseCtx, INFO)
	s = CaptureAsString(func() {
		Debug(ctx, "asdasdas")
	})
	assert.Equalf(t, "", s, "invalid messages")

	ctx = SetLevel(baseCtx, WARNING)
	s = CaptureAsString(func() {
		Debug(ctx, "asdasdas")
	})
	assert.Equalf(t, "", s, "invalid messages")
	assert.False(t, IsDebugEnabled(nil))
}

func TestTrace(t *testing.T) {
	assert.False(t, IsTraceEnabled(baseCtx))
	s := CaptureAsString(func() {
		Trace(baseCtx, "asdasdas")
	})
	assert.Equalf(t, "", s, "should not be error messages")

	ctx := SetLevel(baseCtx, TRACE)
	s = CaptureAsString(func() {
		Trace(ctx, "asdasdas got %d words with prefix %s ", 123, "data")
	})
	assert.Equalf(t, "[2006-01-02T01:02:03.004] [TRACE] [request_id=-] [tenant_id=--] [thread=-] [class=log/log_test.go:12] asdasdas got 123 words with prefix data \n", s, "invalid messages")

	ctx = SetLevel(baseCtx, DEBUG)
	s = CaptureAsString(func() {
		Trace(ctx, "asdasdas")
	})
	assert.Equalf(t, "", s, "invalid messages")

	ctx = SetLevel(baseCtx, INFO)
	s = CaptureAsString(func() {
		Trace(ctx, "asdasdas")
	})
	assert.Equalf(t, "", s, "invalid messages")

	ctx = SetLevel(baseCtx, WARNING)
	s = CaptureAsString(func() {
		Trace(ctx, "asdasdas")
	})
	assert.Equalf(t, "", s, "invalid messages")
	assert.False(t, IsTraceEnabled(nil))
}

func TestExtraTrace(t *testing.T) {
	assert.False(t, IsExtraTraceEnabled(baseCtx))
	s := CaptureAsString(func() {
		ExtraTrace(baseCtx, "asdasdas")
	})
	assert.Equalf(t, "", s, "should not be error messages")

	ctx := SetLevel(baseCtx, EXTRA)
	s = CaptureAsString(func() {
		ExtraTrace(ctx, "asdasdas got %d words with prefix %s ", 123, "data")
	})
	assert.Equalf(t, "[2006-01-02T01:02:03.004] [extra] [request_id=-] [tenant_id=--] [thread=-] [class=log/log_test.go:12] asdasdas got 123 words with prefix data \n", s, "invalid messages")

	ctx = SetLevel(baseCtx, TRACE)
	s = CaptureAsString(func() {
		ExtraTrace(ctx, "asdasdas")
	})
	assert.Equalf(t, "", s, "invalid messages")

	ctx = SetLevel(baseCtx, DEBUG)
	s = CaptureAsString(func() {
		ExtraTrace(ctx, "asdasdas")
	})
	assert.Equalf(t, "", s, "invalid messages")

	ctx = SetLevel(baseCtx, INFO)
	s = CaptureAsString(func() {
		ExtraTrace(ctx, "asdasdas")
	})
	assert.Equalf(t, "", s, "invalid messages")

	ctx = SetLevel(baseCtx, WARNING)
	s = CaptureAsString(func() {
		ExtraTrace(ctx, "asdasdas")
	})
	assert.Equalf(t, "", s, "invalid messages")
	assert.False(t, IsExtraTraceEnabled(nil))
}
