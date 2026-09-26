package hotstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noopObjectStore accepts every PUT.
type noopObjectStore struct{}

func (noopObjectStore) PutFile(context.Context, string, string) error  { return nil }
func (noopObjectStore) PutBytes(context.Context, string, []byte) error { return nil }

// runLoop starts loop with a 1 ms interval, waits until done reports true,
// and stops the loop.
func runLoop(t *testing.T, loop func(context.Context, time.Duration) error, done func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		_ = loop(ctx, time.Millisecond)
	}()
	require.Eventually(t, done, 10*time.Second, time.Millisecond)
	cancel()
	<-exited
}

// TestLoopsCountPasses pins that each maintenance loop counts every pass it
// starts, so loop_errors_total has a denominator; on a healthy store the
// passes move and the errors stay at zero.
func TestLoopsCountPasses(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	t.Run("seal", func(t *testing.T) {
		runLoop(t, store.RunSealLoop, func() bool { return store.SealPasses() >= 2 })
		assert.Zero(t, store.SealLoopErrors(), "SealLoopErrors")
	})
	t.Run("janitor", func(t *testing.T) {
		runLoop(t, store.RunJanitorLoop, func() bool { return store.JanitorPasses() >= 2 })
		assert.Zero(t, store.JanitorLoopErrors(), "JanitorLoopErrors")
	})
	t.Run("upload", func(t *testing.T) {
		u := NewUploader(store, noopObjectStore{})
		runLoop(t, u.Run, func() bool { return u.Passes() >= 2 })
		assert.Zero(t, u.LoopErrors(), "LoopErrors")
	})
}

// TestLoopsCountFailedPasses is the negative control of TestLoopsCountPasses:
// on a closed store every pass fails, and the errors advance with the passes.
func TestLoopsCountFailedPasses(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, store.Close())

	t.Run("seal", func(t *testing.T) {
		runLoop(t, store.RunSealLoop, func() bool { return store.SealLoopErrors() >= 2 })
		assert.GreaterOrEqual(t, store.SealPasses(), store.SealLoopErrors(), "SealPasses against SealLoopErrors")
	})
	t.Run("janitor", func(t *testing.T) {
		runLoop(t, store.RunJanitorLoop, func() bool { return store.JanitorLoopErrors() >= 2 })
		assert.GreaterOrEqual(t, store.JanitorPasses(), store.JanitorLoopErrors(), "JanitorPasses against JanitorLoopErrors")
	})
	t.Run("upload", func(t *testing.T) {
		u := NewUploader(store, noopObjectStore{})
		runLoop(t, u.Run, func() bool { return u.LoopErrors() >= 2 })
		assert.GreaterOrEqual(t, u.Passes(), u.LoopErrors(), "Passes against LoopErrors")
	})
}
