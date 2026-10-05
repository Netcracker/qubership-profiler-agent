package health

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const progressInterval = time.Millisecond

func readyBody(t *testing.T, g *Gate) string {
	t.Helper()
	return get(t, g, "/internal/v1/health/ready").Body.String()
}

func TestTrackProgressUpdatesDetails(t *testing.T) {
	g := NewGate("/internal/v1")
	g.Set(StateRecovery, "recovering the hot store")
	stop := g.TrackProgress(context.Background(), StateRecovery, progressInterval,
		func() string { return "2 of 5 pod-restarts" })
	defer stop()

	require.Eventually(t, func() bool {
		return strings.Contains(readyBody(t, g), "2 of 5 pod-restarts")
	}, 10*time.Second, progressInterval, "the RECOVERY details never carried the progress string")
	assert.JSONEq(t, `{"state":"RECOVERY","details":"2 of 5 pod-restarts"}`, readyBody(t, g))
}

// TestTrackProgressStopJoins pins that stop returns only after the refresh
// has exited: describe is not called once stop returns, and READY keeps its
// empty details.
func TestTrackProgressStopJoins(t *testing.T) {
	g := NewGate("/internal/v1")
	g.Set(StateRecovery, "recovering the hot store")
	var calls atomic.Int64
	stop := g.TrackProgress(context.Background(), StateRecovery, progressInterval, func() string {
		calls.Add(1)
		return "in progress"
	})
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, 10*time.Second, progressInterval)

	stop()
	callsAtStop := calls.Load()
	g.Set(StateReady, "")
	time.Sleep(20 * progressInterval)

	assert.Equal(t, callsAtStop, calls.Load(), "describe calls after stop returned")
	assert.JSONEq(t, `{"state":"READY"}`, readyBody(t, g))
}

// TestTrackProgressStopWaitsForInFlightRefresh holds a describe call open and
// pins that stop does not return until it finishes. TestTrackProgressStopJoins
// catches a stop that only cancels by chance, when a tick races the cancel;
// this one catches it on every run.
func TestTrackProgressStopWaitsForInFlightRefresh(t *testing.T) {
	g := NewGate("/internal/v1")
	g.Set(StateRecovery, "recovering the hot store")
	entered := make(chan struct{})
	release := make(chan struct{})
	var first sync.Once
	stop := g.TrackProgress(context.Background(), StateRecovery, progressInterval, func() string {
		first.Do(func() {
			close(entered)
			<-release
		})
		return "in progress"
	})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("describe was never called")
	}

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		close(release)
		t.Fatal("stop returned while a describe call was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("stop did not return after the describe call finished")
	}
}

// TestTrackProgressLeavesOtherStates pins that a refresh that finds the gate
// in another state keeps that state's details.
func TestTrackProgressLeavesOtherStates(t *testing.T) {
	g := NewGate("/internal/v1")
	g.Set(StateRecovery, "recovering the hot store")
	var calls atomic.Int64
	stop := g.TrackProgress(context.Background(), StateRecovery, progressInterval, func() string {
		calls.Add(1)
		return "in progress"
	})
	defer stop()

	g.Set(StateFatal, "boom")
	callsAtFatal := calls.Load()
	require.Eventually(t, func() bool { return calls.Load() >= callsAtFatal+2 }, 10*time.Second, progressInterval,
		"the refresh stopped ticking, so the state guard was never exercised")
	assert.JSONEq(t, `{"state":"FATAL","details":"boom"}`, readyBody(t, g))
}

func TestTrackProgressStopTwice(t *testing.T) {
	g := NewGate("/internal/v1")
	stop := g.TrackProgress(context.Background(), StateRecovery, progressInterval, func() string { return "" })
	stop()
	assert.NotPanics(t, stop)
}
