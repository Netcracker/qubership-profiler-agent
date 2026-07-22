package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock drives the scheduler deterministically: After() fires instantly
// while advancing the fake time, so a test runs a multi-minute schedule in
// microseconds and still observes correct ordering and timestamps.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	at := c.now
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- at
	return ch
}

type fakeKube struct {
	mu       sync.Mutex
	deleted  []string
	replicas map[string]int32
	scaleLog []string
	// readyAfterPolls makes PodReady answer false this many times after a
	// delete before turning true again; negative means never ready.
	readyAfterPolls int
	pendingPolls    int
	deleteErr       error
}

func (k *fakeKube) key(ns, name string) string { return ns + "/" + name }

func (k *fakeKube) DeletePod(ctx context.Context, ns, pod string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.deleteErr != nil {
		return k.deleteErr
	}
	k.deleted = append(k.deleted, k.key(ns, pod))
	k.pendingPolls = k.readyAfterPolls
	return nil
}

func (k *fakeKube) PodReady(ctx context.Context, ns, pod string) (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.pendingPolls < 0 {
		return false, nil
	}
	if k.pendingPolls > 0 {
		k.pendingPolls--
		return false, nil
	}
	return true, nil
}

func (k *fakeKube) GetScale(ctx context.Context, ns, kind, name string) (int32, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.replicas[k.key(ns, name)], nil
}

func (k *fakeKube) SetScale(ctx context.Context, ns, kind, name string, replicas int32) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.replicas[k.key(ns, name)] = replicas
	k.scaleLog = append(k.scaleLog, fmt.Sprintf("%s=%d", k.key(ns, name), replicas))
	return nil
}

type fakeToxi struct {
	mu      sync.Mutex
	toxics  map[string][]string // proxy -> toxic names
	created []string
	deleted []string
	failOn  string // toxic name whose creation fails
}

func newFakeToxi() *fakeToxi { return &fakeToxi{toxics: map[string][]string{}} }

func (t *fakeToxi) CreateToxic(ctx context.Context, proxy, name, typ string, attrs map[string]any) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if name == t.failOn {
		return fmt.Errorf("boom on %s", name)
	}
	t.toxics[proxy] = append(t.toxics[proxy], name)
	t.created = append(t.created, name)
	return nil
}

func (t *fakeToxi) DeleteToxic(ctx context.Context, proxy, name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	kept := t.toxics[proxy][:0]
	for _, n := range t.toxics[proxy] {
		if n != name {
			kept = append(kept, n)
		}
	}
	t.toxics[proxy] = kept
	t.deleted = append(t.deleted, name)
	return nil
}

func (t *fakeToxi) ListToxicNames(ctx context.Context, proxy string) ([]string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.toxics[proxy]...), nil
}

func testFaultRunner(t *testing.T, faults []FaultSpec, kube kubeFaults, toxi toxiFaults) (*faultRunner, *fakeClock, string) {
	t.Helper()
	dir := t.TempDir()
	log, err := newFaultLog(filepath.Join(dir, "faults.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	reg, err := openFaultRegistry(dir, "test-run")
	require.NoError(t, err)
	clock := newFakeClock()
	fr := newFaultRunner(faults, log, reg, kube, toxi, clock)
	return fr, clock, dir
}

func readEvents(t *testing.T, dir string) []faultEvent {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "faults.jsonl"))
	require.NoError(t, err)
	var out []faultEvent
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var ev faultEvent
		require.NoError(t, json.Unmarshal([]byte(line), &ev))
		out = append(out, ev)
	}
	return out
}

func eventSeq(events []faultEvent, faultID string) []string {
	var seq []string
	for _, ev := range events {
		if ev.FaultID == faultID || faultID == "" {
			seq = append(seq, ev.Event)
		}
	}
	return seq
}

func validated(t *testing.T, f FaultSpec) FaultSpec {
	t.Helper()
	require.NoError(t, f.validate(true))
	return f
}

func TestCrashloopIsReadyGated(t *testing.T) {
	kube := &fakeKube{readyAfterPolls: 2}
	f := validated(t, FaultSpec{
		Name: "kill-collector", At: duration(time.Minute), Action: "pod-delete",
		Target: FaultTarget{Namespace: "ns", Pod: "collector-1"},
		Repeat: &FaultRepeat{Every: duration(90 * time.Second), Count: 3, ReadyTimeout: duration(5 * time.Minute)},
	})
	fr, clock, dir := testFaultRunner(t, []FaultSpec{f}, kube, nil)
	fr.start(context.Background(), clock.Now())
	fr.wait()

	require.NoError(t, fr.err())
	assert.Len(t, kube.deleted, 3, "every cycle killed the pod")
	events := readEvents(t, dir)
	// Per injection: started, ended, ready — and per-injection fault ids.
	assert.Equal(t, []string{"started", "ended", "ready"}, eventSeq(events, "kill-collector-001"))
	assert.Equal(t, []string{"started", "ended", "ready"}, eventSeq(events, "kill-collector-003"))
	// The ready event precedes the next cycle's started: the loop is gated
	// on observed recovery, not on a timer.
	var seq []string
	for _, ev := range events {
		if ev.Event == "started" || ev.Event == "ready" {
			seq = append(seq, ev.FaultID+":"+ev.Event)
		}
	}
	assert.Equal(t, []string{
		"kill-collector-001:started", "kill-collector-001:ready",
		"kill-collector-002:started", "kill-collector-002:ready",
		"kill-collector-003:started", "kill-collector-003:ready",
	}, seq)
}

func TestCrashloopReadyTimeoutFailsRecovery(t *testing.T) {
	kube := &fakeKube{readyAfterPolls: -1} // never ready again
	f := validated(t, FaultSpec{
		Name: "kill-collector", At: duration(time.Minute), Action: "pod-delete",
		Target: FaultTarget{Namespace: "ns", Pod: "collector-1"},
		Repeat: &FaultRepeat{Every: duration(90 * time.Second), Count: 5, ReadyTimeout: duration(time.Minute)},
	})
	fr, clock, dir := testFaultRunner(t, []FaultSpec{f}, kube, nil)
	fr.start(context.Background(), clock.Now())
	fr.wait()

	require.Error(t, fr.err(), "a target that never turns READY is a failed recovery")
	assert.Contains(t, fr.err().Error(), "failed recovery")
	assert.Len(t, kube.deleted, 1, "injections stop after the failed recovery")
	events := readEvents(t, dir)
	assert.Contains(t, eventSeq(events, "kill-collector-001"), "error")
}

func TestScaleWritesRegistryBeforeActingAndRevertsAfter(t *testing.T) {
	kube := &fakeKube{replicas: map[string]int32{"ns/minio": 1}}
	to := int32(0)
	f := validated(t, FaultSpec{
		Name: "s3-outage", At: duration(time.Minute), Action: "scale",
		Target:   FaultTarget{Namespace: "ns", Kind: "statefulset", Name: "minio"},
		To:       &to,
		Duration: duration(15 * time.Minute),
		Expects:  []string{"refused-bytes"},
	})
	fr, clock, dir := testFaultRunner(t, []FaultSpec{f}, kube, nil)
	fr.start(context.Background(), clock.Now())
	fr.wait()

	require.NoError(t, fr.err())
	assert.Equal(t, []string{"ns/minio=0", "ns/minio=1"}, kube.scaleLog, "scaled down, then reverted to the prior count")
	assert.Equal(t, []string{"scheduled", "started", "revert-started", "reverted"}, eventSeq(readEvents(t, dir), ""))
	// The registry is empty again: the revert removed the entry (and the
	// empty file disappears).
	_, err := os.Stat(filepath.Join(activeFaultsDir(dir), "test-run.json"))
	assert.True(t, os.IsNotExist(err), "a clean revert leaves no registry file")
}

func TestToxicsCreatedAndRevertedByPrefix(t *testing.T) {
	toxi := newFakeToxi()
	f := validated(t, FaultSpec{
		Name: "s3-slow", At: duration(time.Minute), Action: "toxics",
		Target: FaultTarget{Proxy: "s3"},
		Toxics: []ToxicSpec{
			{Type: "latency", Attributes: map[string]any{"latency": 2000}},
			{Type: "bandwidth", Attributes: map[string]any{"rate": 64}},
		},
		Duration: duration(10 * time.Minute),
	})
	fr, clock, _ := testFaultRunner(t, []FaultSpec{f}, nil, toxi)
	fr.start(context.Background(), clock.Now())
	fr.wait()

	require.NoError(t, fr.err())
	assert.Equal(t, []string{"s3-slow-0", "s3-slow-1"}, toxi.created, "toxic names carry the faultId prefix")
	assert.ElementsMatch(t, []string{"s3-slow-0", "s3-slow-1"}, toxi.deleted)
	assert.Empty(t, toxi.toxics["s3"], "the proxy is clean after the revert")
}

func TestInjectionFailureTurnsRunInvalid(t *testing.T) {
	kube := &fakeKube{deleteErr: fmt.Errorf("forbidden")}
	f := validated(t, FaultSpec{
		Name: "kill-collector", At: duration(time.Minute), Action: "pod-delete",
		Target: FaultTarget{Namespace: "ns", Pod: "collector-1"},
	})
	fr, clock, _ := testFaultRunner(t, []FaultSpec{f}, kube, nil)
	fr.start(context.Background(), clock.Now())
	fr.wait()
	require.Error(t, fr.err(), "a failed injection must surface, not just land in the log")
}

func TestExpectedWindowsTrackFaultLifecycle(t *testing.T) {
	kube := &fakeKube{replicas: map[string]int32{"ns/minio": 1}}
	to := int32(0)
	f := validated(t, FaultSpec{
		Name: "s3-outage", At: duration(time.Minute), Action: "scale",
		Target:   FaultTarget{Namespace: "ns", Kind: "statefulset", Name: "minio"},
		To:       &to,
		Duration: duration(15 * time.Minute),
		Expects:  []string{"refused-bytes"},
		Settle:   duration(5 * time.Minute),
	})
	fr, clock, _ := testFaultRunner(t, []FaultSpec{f}, kube, nil)
	holdStart := clock.Now()
	fr.start(context.Background(), holdStart)
	fr.wait()

	// The fault ran at +1m for 15m with 5m settle: [1m, 21m] is expected.
	assert.False(t, fr.inWindow("refused-bytes", holdStart.Add(30*time.Second)),
		"before the injection nothing is expected")
	assert.True(t, fr.inWindow("refused-bytes", holdStart.Add(10*time.Minute)))
	assert.True(t, fr.inWindow("refused-bytes", holdStart.Add(20*time.Minute)), "the settle tail is expected")
	assert.False(t, fr.inWindow("refused-bytes", holdStart.Add(25*time.Minute)),
		"past the settle tail nothing is expected")
	assert.False(t, fr.inWindow("restarts", holdStart.Add(10*time.Minute)),
		"only the declared signals get windows")
}

func TestExcludeFaultWindows(t *testing.T) {
	kube := &fakeKube{replicas: map[string]int32{"ns/minio": 1}}
	to := int32(0)
	f := validated(t, FaultSpec{
		Name: "s3-outage", At: duration(time.Minute), Action: "scale",
		Target:   FaultTarget{Namespace: "ns", Kind: "statefulset", Name: "minio"},
		To:       &to,
		Duration: duration(15 * time.Minute),
		Expects:  []string{"refused-bytes"},
		Settle:   duration(5 * time.Minute),
	})
	fr, clock, _ := testFaultRunner(t, []FaultSpec{f}, kube, nil)
	holdStart := clock.Now()
	fr.start(context.Background(), holdStart)
	fr.wait()

	pts := []Point{
		{At: holdStart.Add(30 * time.Second), Value: 0},
		{At: holdStart.Add(10 * time.Minute), Value: 4096},
		{At: holdStart.Add(30 * time.Minute), Value: 0},
	}
	kept := excludeFaultWindows(pts, "refused-bytes", fr)
	require.Len(t, kept, 2, "the in-window sample is excluded from the detector")
	assert.Equal(t, pts[0].At, kept[0].At)
	assert.Equal(t, pts[2].At, kept[1].At)
	assert.Len(t, excludeFaultWindows(pts, "ack-errors", fr), 3,
		"other signals keep every sample")
}

func TestRevertStaleFaultsSkipsLiveLease(t *testing.T) {
	dir := t.TempDir()
	// A dead run left a toxics entry; a live run holds the lease.
	reg, err := openFaultRegistry(dir, "dead-run")
	require.NoError(t, err)
	require.NoError(t, reg.add(registryEntry{
		FaultID: "s3-slow", Action: "toxics",
		Target: FaultTarget{Proxy: "s3"}, ToxicNames: []string{"s3-slow-0"},
		ToxiproxyURL: "http://127.0.0.1:1", // unreachable: reverting it must FAIL, proving it was attempted
	}))
	liveReg, err := openFaultRegistry(dir, "live-run")
	require.NoError(t, err)
	require.NoError(t, liveReg.add(registryEntry{
		FaultID: "other", Action: "toxics",
		Target: FaultTarget{Proxy: "agent"}, ToxicNames: []string{"other-0"},
		ToxiproxyURL: "http://127.0.0.1:1",
	}))

	lock, err := acquireStandLock(context.Background(), dir, "live-run")
	require.NoError(t, err)
	defer lock.release()

	// The dead run's entry is attempted (and fails on the unreachable
	// endpoint); the live run's entry is never touched.
	_, err = revertStaleFaults(context.Background(), dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dead-run")
	assert.NotContains(t, err.Error(), "live-run")
}

func TestStandLockRefusesForeignLiveLease(t *testing.T) {
	dir := t.TempDir()
	lock, err := acquireStandLock(context.Background(), dir, "run-a")
	require.NoError(t, err)
	defer lock.release()

	_, err = acquireStandLock(context.Background(), dir, "run-b")
	require.Error(t, err, "a live foreign lease refuses the start")
	assert.Contains(t, err.Error(), "run-a")

	// The same testid may re-acquire (a retried runner).
	again, err := acquireStandLock(context.Background(), dir, "run-a")
	require.NoError(t, err)
	again.release()
}

func TestStandLockExpiredLeaseIsTakeable(t *testing.T) {
	dir := t.TempDir()
	stale := leaseFile{TestID: "dead", PID: 1, AcquiredAt: time.Now().Add(-time.Hour),
		RenewedAt: time.Now().Add(-time.Hour), TTLSec: 90}
	raw, err := json.Marshal(stale)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(lockPath(dir), raw, 0o644))

	lock, err := acquireStandLock(context.Background(), dir, "run-b")
	require.NoError(t, err, "an expired lease must not block the stand")
	lock.release()
}

func TestFaultLogLinesAreCompleteJSON(t *testing.T) {
	kube := &fakeKube{}
	f := validated(t, FaultSpec{
		Name: "kill", At: duration(time.Minute), Action: "pod-delete",
		Target: FaultTarget{Namespace: "ns", Pod: "p"},
	})
	fr, clock, dir := testFaultRunner(t, []FaultSpec{f}, kube, nil)
	fr.start(context.Background(), clock.Now())
	fr.wait()
	// readEvents unmarshals every line strictly — a torn or concatenated
	// line would fail the test.
	events := readEvents(t, dir)
	require.NotEmpty(t, events)
	for _, ev := range events {
		assert.NotEmpty(t, ev.FaultID)
		assert.False(t, ev.At.IsZero())
		assert.False(t, ev.ScheduledAt.IsZero(), "scheduledAt and at are separate fields")
	}
}

func TestFaultSpecValidation(t *testing.T) {
	base := func() FaultSpec {
		return FaultSpec{Name: "ok", At: duration(time.Minute), Action: "pod-delete",
			Target: FaultTarget{Namespace: "ns", Pod: "p"}}
	}
	good := base()
	require.NoError(t, good.validate(false))
	assert.Equal(t, 5*time.Minute, good.Settle.std(), "settle defaults to 5m")

	bad := base()
	bad.Name = "Kill#1"
	assert.Error(t, bad.validate(false), "unsafe characters in the name")

	bad = base()
	bad.At = 0
	assert.Error(t, bad.validate(false), "at is required")

	bad = base()
	bad.Repeat = &FaultRepeat{Every: duration(time.Minute), Count: 2}
	bad.Duration = duration(time.Minute)
	assert.Error(t, bad.validate(false), "repeat and duration exclude each other")

	bad = base()
	bad.Expects = []string{"nonsense"}
	assert.Error(t, bad.validate(false), "expects vocabulary is closed")

	scale := FaultSpec{Name: "s", At: duration(time.Minute), Action: "scale",
		Target: FaultTarget{Namespace: "ns", Kind: "statefulset", Name: "minio"}}
	assert.Error(t, scale.validate(false), "scale needs to")

	tox := FaultSpec{Name: "t", At: duration(time.Minute), Action: "toxics",
		Target: FaultTarget{Proxy: "s3"}, Toxics: []ToxicSpec{{Type: "latency"}},
		Duration: duration(time.Minute)}
	assert.Error(t, tox.validate(false), "toxics needs endpoints.toxiproxy")
	assert.NoError(t, tox.validate(true))
}
