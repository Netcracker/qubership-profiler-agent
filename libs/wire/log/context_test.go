package log

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestContext(t *testing.T) {
	ctx := SetLevel(Context("testCtx"), DEBUG)
	assert.NotNil(t, ctx)
	assert.Equal(t, DEBUG, GetLevel(ctx))
	assert.Equal(t, "testCtx", GetContextName(ctx))
	assert.Equal(t, "", GetContextName(context.Background()))
	assert.Equal(t, "", GetContextName(context.WithValue(ctx, ContextKey, 123))) // invalid value
}

func TestWithSink(t *testing.T) {
	var got []string
	ctx := WithSink(SetLevel(baseCtx, INFO), func(level, msg string) { got = append(got, level+" "+msg) })
	out := CaptureAsString(func() {
		Info(ctx, "pod %s", "a")
		Debug(ctx, "below the level")
		Error(ctx, fmt.Errorf("boom"), "read failed")
	}, true)
	assert.Empty(t, out, "a sink replaces stdout")
	assert.Equal(t, []string{"INFO pod a", "ERROR read failed: boom"}, got)
}
