package log

import (
	"context"
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
