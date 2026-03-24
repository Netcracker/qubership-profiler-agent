package log

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const (
	TimeFormat = "2006-01-02T15:04:05.999"
)

var (
	ShortFormat = "[%s][%s][%s:%d] %s\n"                                                        // [time][level][source] msg
	Format      = "[%s] [%s] [request_id=%s] [tenant_id=-%s] [thread=%s] [class=%s/%s:%d] %s\n" // [time] [level] [request_id=-] [tenant_id=-] [thread=-] [class=-] msg
	isTest      = false
)

func header(ctx context.Context, skip int, level level, format string) string {
	_, filename, line, _ := runtime.Caller(skip)
	//timeFormat := time.Now().Format(time.TimeOnly)
	timeFormat := time.Now().Format(TimeFormat)
	if isTest {
		timeFormat = "2006-01-02T01:02:03.004"
		line = 12
	}
	request, tenant, ctxName := "-", "-", "-"
	if ctx != nil {
		if v, ok := ctx.Value(ContextKey).(string); ok && len(v) > 0 {
			ctxName = v
		}
		if v, ok := ctx.Value(RequestId).(string); ok && len(v) > 0 {
			ctxName = v
		}
	}
	file := filepath.Base(filename)
	pkg := filepath.Base(filepath.Dir(filename))
	return fmt.Sprintf(Format, timeFormat, level, request, tenant, ctxName, pkg, file, line, format)
}

func CaptureAsString(f func(), silent ...bool) string { // for unit testing
	was := isTest
	orig := os.Stdout

	r, w, _ := os.Pipe()
	isTest = true
	os.Stdout = w
	f()
	os.Stdout = orig
	isTest = was

	w.Close()
	out, _ := io.ReadAll(r)
	if len(silent) == 0 || !silent[0] {
		os.Stdout.Write(out)
	}
	return string(out)
}
