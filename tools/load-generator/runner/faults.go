// Fault-injection layer (doc/run-orchestration.md, "Fault injection"): the
// scheduler that executes a spec's faults: block during the hold, the atomic
// event log the checker consumes live, and the durable-revert registry that
// survives a killed runner.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// faultEvent is one faults.jsonl line — atomic, written the moment it
// happens. scheduledAt is the plan, at is the fact, error events carry the
// failure in detail; never folded into one field.
type faultEvent struct {
	FaultID     string      `json:"faultId"`
	Name        string      `json:"name"`
	Event       string      `json:"event"` // scheduled|started|ended|revert-started|reverted|ready|error
	At          time.Time   `json:"at"`
	ScheduledAt time.Time   `json:"scheduledAt"`
	Action      string      `json:"action"`
	Target      FaultTarget `json:"target"`
	Expects     []string    `json:"expects,omitempty"`
	SettleSec   float64     `json:"settleSec"`
	// RestartBudget is the injection's §8.8 unit budget (doc/checker.md).
	RestartBudget int    `json:"restartBudget"`
	Detail        string `json:"detail,omitempty"`
}

// faultLog appends events to faults.jsonl. One JSON object per line; the
// write is a single O_APPEND syscall so a concurrent reader sees whole lines
// (and must tolerate a torn last line by re-reading it next tick).
type faultLog struct {
	mu   sync.Mutex
	file *os.File
}

func newFaultLog(path string) (*faultLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &faultLog{file: f}, nil
}

func (l *faultLog) append(ev faultEvent) error {
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err = l.file.Write(append(raw, '\n'))
	return err
}

func (l *faultLog) Close() error { return l.file.Close() }

// registryEntry is the prior state a stateful action must restore. The
// registry lives OUTSIDE the run directory (runs/.active-faults/<testid>.json)
// so the next run's preflight finds a dead run's leftovers.
type registryEntry struct {
	FaultID       string      `json:"faultId"`
	Action        string      `json:"action"`
	Target        FaultTarget `json:"target"`
	PriorReplicas int32       `json:"priorReplicas,omitempty"`
	ToxicNames    []string    `json:"toxicNames,omitempty"`
	// ToxiproxyURL rides along so a standalone -revert-faults can reach the
	// proxy without the dead run's spec.
	ToxiproxyURL string `json:"toxiproxyUrl,omitempty"`
}

type registryFile struct {
	TestID  string          `json:"testid"`
	Entries []registryEntry `json:"entries"`
}

// faultRegistry persists active-fault state atomically (tmp + rename).
type faultRegistry struct {
	path string
	mu   sync.Mutex
	data registryFile
}

func activeFaultsDir(outputs string) string { return filepath.Join(outputs, ".active-faults") }

func openFaultRegistry(outputs, testid string) (*faultRegistry, error) {
	dir := activeFaultsDir(outputs)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	r := &faultRegistry{path: filepath.Join(dir, testid+".json"), data: registryFile{TestID: testid}}
	raw, err := os.ReadFile(r.path)
	switch {
	case err == nil:
		if err := json.Unmarshal(raw, &r.data); err != nil {
			return nil, fmt.Errorf("%s: %w", r.path, err)
		}
	case !os.IsNotExist(err):
		return nil, err
	}
	return r, nil
}

func (r *faultRegistry) add(e registryEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data.Entries = append(r.data.Entries, e)
	return r.flush()
}

func (r *faultRegistry) remove(faultID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := r.data.Entries[:0]
	for _, e := range r.data.Entries {
		if e.FaultID != faultID {
			kept = append(kept, e)
		}
	}
	r.data.Entries = kept
	return r.flush()
}

func (r *faultRegistry) entries() []registryEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]registryEntry(nil), r.data.Entries...)
}

// flush writes the registry atomically; an empty registry removes the file
// so -revert-faults and the preflight sweep see only real leftovers.
func (r *faultRegistry) flush() error {
	if len(r.data.Entries) == 0 {
		if err := os.Remove(r.path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	raw, err := json.MarshalIndent(r.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, r.path)
}

// kubeFaults is the k8s slice the drivers need; the production implementation
// is client-go, tests use a fake.
type kubeFaults interface {
	DeletePod(ctx context.Context, namespace, pod string) error
	PodReady(ctx context.Context, namespace, pod string) (bool, error)
	GetScale(ctx context.Context, namespace, kind, name string) (int32, error)
	SetScale(ctx context.Context, namespace, kind, name string, replicas int32) error
}

// toxiFaults is the toxiproxy slice; production talks REST, tests fake it.
type toxiFaults interface {
	CreateToxic(ctx context.Context, proxy, name, typ string, attrs map[string]any) error
	DeleteToxic(ctx context.Context, proxy, name string) error
	ListToxicNames(ctx context.Context, proxy string) ([]string, error)
}

// faultClock abstracts time for the scheduler tests.
type faultClock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// window is one expected-effects interval of an executed fault.
type faultWindow struct {
	from time.Time
	to   time.Time // zero while the fault is active: the window is open
}

// faultRunner executes the spec's schedule during the hold and answers two
// questions for the hold loop: "which samples fall into an expected window
// of signal X" and "did any injection or revert fail".
type faultRunner struct {
	spec         []FaultSpec
	log          *faultLog
	registry     *faultRegistry
	kube         kubeFaults
	toxi         toxiFaults
	toxiproxyURL string
	clock        faultClock

	mu      sync.Mutex
	windows map[string][]faultWindow // expects signal -> executed windows (settle already applied on close)
	failure error                    // first injection/revert failure; turns the run invalid
	wg      sync.WaitGroup
}

func newFaultRunner(faults []FaultSpec, log *faultLog, reg *faultRegistry,
	kube kubeFaults, toxi toxiFaults, clock faultClock) *faultRunner {
	return &faultRunner{spec: faults, log: log, registry: reg, kube: kube, toxi: toxi,
		clock: clock, windows: map[string][]faultWindow{}}
}

// start launches one goroutine per fault, all offsets relative to holdStart.
func (fr *faultRunner) start(ctx context.Context, holdStart time.Time) {
	for i := range fr.spec {
		f := fr.spec[i]
		fr.wg.Add(1)
		go func() {
			defer fr.wg.Done()
			fr.runFault(ctx, holdStart, f)
		}()
	}
}

// wait blocks until every fault goroutine finished (reverts included).
func (fr *faultRunner) wait() { fr.wg.Wait() }

// err reports the first injection/revert failure.
func (fr *faultRunner) err() error {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	return fr.failure
}

// inWindow reports whether t falls into an expected window of the named
// signal. An open window (fault still active) extends to now; a closed one
// includes the settle tail (already folded into to).
func (fr *faultRunner) inWindow(signal string, t time.Time) bool {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	for _, w := range fr.windows[signal] {
		if t.Before(w.from) {
			continue
		}
		if w.to.IsZero() || !t.After(w.to) {
			return true
		}
	}
	return false
}

func (fr *faultRunner) fail(err error) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	if fr.failure == nil {
		fr.failure = err
	}
}

func (fr *faultRunner) openWindow(f FaultSpec, from time.Time) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	for _, sig := range f.Expects {
		fr.windows[sig] = append(fr.windows[sig], faultWindow{from: from})
	}
}

// closeWindow seals the newest open window of every expected signal at
// end + settle.
func (fr *faultRunner) closeWindow(f FaultSpec, end time.Time) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	to := end.Add(f.Settle.std())
	for _, sig := range f.Expects {
		ws := fr.windows[sig]
		for i := len(ws) - 1; i >= 0; i-- {
			if ws[i].to.IsZero() {
				ws[i].to = to
				break
			}
		}
	}
}

func (fr *faultRunner) event(f FaultSpec, faultID, event string, scheduledAt time.Time, detail string) {
	ev := faultEvent{
		FaultID: faultID, Name: f.Name, Event: event,
		At: fr.clock.Now(), ScheduledAt: scheduledAt,
		Action: f.Action, Target: f.Target, Expects: f.Expects,
		SettleSec: f.Settle.std().Seconds(), RestartBudget: f.RestartBudget, Detail: detail,
	}
	if err := fr.log.append(ev); err != nil {
		fr.fail(fmt.Errorf("fault %s: event log: %w", faultID, err))
	}
	fmt.Printf("runner: fault %s %s%s\n", faultID, event, suffixIf(detail))
}

func suffixIf(detail string) string {
	if detail == "" {
		return ""
	}
	return ": " + detail
}

func (fr *faultRunner) runFault(ctx context.Context, holdStart time.Time, f FaultSpec) {
	scheduledAt := holdStart.Add(f.At.std())
	fr.event(f, f.Name, "scheduled", scheduledAt, "")
	if !fr.sleepUntil(ctx, scheduledAt) {
		return
	}
	switch f.Action {
	case "pod-delete":
		fr.runPodDelete(ctx, f, scheduledAt)
	case "scale":
		fr.runScale(ctx, f, scheduledAt)
	case "toxics":
		fr.runToxics(ctx, f, scheduledAt)
	}
}

func (fr *faultRunner) sleepUntil(ctx context.Context, at time.Time) bool {
	d := at.Sub(fr.clock.Now())
	if d <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-fr.clock.After(d):
		return true
	}
}

// runPodDelete executes one kill, or a ready-gated crashloop under repeat:
// the next injection is scheduled only after the target is observed READY
// again plus every; missing readyTimeout is a failed recovery.
func (fr *faultRunner) runPodDelete(ctx context.Context, f FaultSpec, scheduledAt time.Time) {
	count := 1
	if f.Repeat != nil {
		count = f.Repeat.Count
	}
	for i := 1; i <= count; i++ {
		faultID := f.Name
		if f.Repeat != nil {
			faultID = fmt.Sprintf("%s-%03d", f.Name, i)
		}
		fr.openWindow(f, fr.clock.Now())
		fr.event(f, faultID, "started", scheduledAt, "")
		if err := fr.kube.DeletePod(ctx, f.Target.Namespace, f.Target.Pod); err != nil {
			fr.event(f, faultID, "error", scheduledAt, err.Error())
			fr.fail(fmt.Errorf("fault %s: pod-delete: %w", faultID, err))
			fr.closeWindow(f, fr.clock.Now())
			return
		}
		fr.event(f, faultID, "ended", scheduledAt, "")
		fr.closeWindow(f, fr.clock.Now())

		readyBy := fr.clock.Now().Add(readyTimeout(f))
		ready, err := fr.awaitReady(ctx, f, readyBy)
		if err != nil {
			fr.event(f, faultID, "error", scheduledAt, err.Error())
			fr.fail(fmt.Errorf("fault %s: %w", faultID, err))
			return
		}
		if !ready {
			return // ctx cancelled
		}
		fr.event(f, faultID, "ready", scheduledAt, "")
		if i < count {
			scheduledAt = fr.clock.Now().Add(f.Repeat.Every.std())
			if !fr.sleepUntil(ctx, scheduledAt) {
				return
			}
		}
	}
}

func readyTimeout(f FaultSpec) time.Duration {
	if f.Repeat != nil {
		return f.Repeat.ReadyTimeout.std()
	}
	return 5 * time.Minute
}

// awaitReady polls the target pod until READY, the deadline, or ctx end.
// (false, nil) means ctx ended; past the deadline it returns the
// failed-recovery error.
func (fr *faultRunner) awaitReady(ctx context.Context, f FaultSpec, deadline time.Time) (bool, error) {
	for {
		ready, err := fr.kube.PodReady(ctx, f.Target.Namespace, f.Target.Pod)
		if err == nil && ready {
			return true, nil
		}
		if ctx.Err() != nil {
			return false, nil
		}
		if fr.clock.Now().After(deadline) {
			return false, fmt.Errorf("pod %s/%s not READY within %s — failed recovery",
				f.Target.Namespace, f.Target.Pod, readyTimeout(f))
		}
		select {
		case <-ctx.Done():
			return false, nil
		case <-fr.clock.After(5 * time.Second):
		}
	}
}

// runScale scales the target down (or up) and durably reverts after the
// window. The prior replica count lands in the registry BEFORE the action:
// a runner killed mid-window leaves the entry for -revert-faults.
func (fr *faultRunner) runScale(ctx context.Context, f FaultSpec, scheduledAt time.Time) {
	prior, err := fr.kube.GetScale(ctx, f.Target.Namespace, f.Target.Kind, f.Target.Name)
	if err != nil {
		fr.event(f, f.Name, "error", scheduledAt, err.Error())
		fr.fail(fmt.Errorf("fault %s: read scale: %w", f.Name, err))
		return
	}
	if err := fr.registry.add(registryEntry{
		FaultID: f.Name, Action: f.Action, Target: f.Target, PriorReplicas: prior,
	}); err != nil {
		fr.fail(fmt.Errorf("fault %s: registry: %w", f.Name, err))
		return
	}
	fr.openWindow(f, fr.clock.Now())
	fr.event(f, f.Name, "started", scheduledAt, fmt.Sprintf("replicas %d -> %d", prior, *f.To))
	if err := fr.kube.SetScale(ctx, f.Target.Namespace, f.Target.Kind, f.Target.Name, *f.To); err != nil {
		fr.event(f, f.Name, "error", scheduledAt, err.Error())
		fr.fail(fmt.Errorf("fault %s: scale: %w", f.Name, err))
		fr.closeWindow(f, fr.clock.Now())
		return
	}

	fr.sleepUntil(ctx, fr.clock.Now().Add(f.Duration.std()))
	// The revert runs even when ctx is done — an aborted run must still
	// restore the stand.
	fr.event(f, f.Name, "revert-started", scheduledAt, "")
	revertCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	if err := fr.kube.SetScale(revertCtx, f.Target.Namespace, f.Target.Kind, f.Target.Name, prior); err != nil {
		fr.event(f, f.Name, "error", scheduledAt, "revert: "+err.Error())
		fr.fail(fmt.Errorf("fault %s: revert: %w", f.Name, err))
		return // the registry keeps the entry for -revert-faults
	}
	if err := fr.registry.remove(f.Name); err != nil {
		fr.fail(fmt.Errorf("fault %s: registry: %w", f.Name, err))
	}
	fr.event(f, f.Name, "reverted", scheduledAt, "")
	fr.closeWindow(f, fr.clock.Now())
}

// runToxics creates the listed toxics (named <faultId>-<idx>) and deletes
// them after the window, with the same durable-revert discipline as scale.
func (fr *faultRunner) runToxics(ctx context.Context, f FaultSpec, scheduledAt time.Time) {
	names := make([]string, len(f.Toxics))
	for i := range f.Toxics {
		names[i] = fmt.Sprintf("%s-%d", f.Name, i)
	}
	if err := fr.registry.add(registryEntry{
		FaultID: f.Name, Action: f.Action, Target: f.Target, ToxicNames: names,
		ToxiproxyURL: fr.toxiproxyURL,
	}); err != nil {
		fr.fail(fmt.Errorf("fault %s: registry: %w", f.Name, err))
		return
	}
	fr.openWindow(f, fr.clock.Now())
	fr.event(f, f.Name, "started", scheduledAt, strings.Join(names, ","))
	for i, tx := range f.Toxics {
		if err := fr.toxi.CreateToxic(ctx, f.Target.Proxy, names[i], tx.Type, tx.Attributes); err != nil {
			fr.event(f, f.Name, "error", scheduledAt, err.Error())
			fr.fail(fmt.Errorf("fault %s: toxic %s: %w", f.Name, names[i], err))
			break
		}
	}

	if fr.err() == nil {
		fr.sleepUntil(ctx, fr.clock.Now().Add(f.Duration.std()))
	}
	fr.event(f, f.Name, "revert-started", scheduledAt, "")
	revertCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	failed := false
	for _, name := range names {
		if err := fr.toxi.DeleteToxic(revertCtx, f.Target.Proxy, name); err != nil {
			fr.event(f, f.Name, "error", scheduledAt, "revert: "+err.Error())
			fr.fail(fmt.Errorf("fault %s: revert toxic %s: %w", f.Name, name, err))
			failed = true
		}
	}
	if failed {
		return // registry entry stays for -revert-faults
	}
	if err := fr.registry.remove(f.Name); err != nil {
		fr.fail(fmt.Errorf("fault %s: registry: %w", f.Name, err))
	}
	fr.event(f, f.Name, "reverted", scheduledAt, "")
	fr.closeWindow(f, fr.clock.Now())
}
