// Expected-failure allowances (doc/checker.md, "Expected failures"): the
// checker re-reads the runner's faults.jsonl on every tick and scopes each
// declared signal to (subject, window, budget). Findings are matched by
// their observation time, never by the evaluation time.
package main

import (
	"bufio"
	"encoding/json"
	"os"
	"sort"
	"time"
)

// faultLogEvent is the subset of a runner fault event the checker consumes.
type faultLogEvent struct {
	FaultID string    `json:"faultId"`
	Event   string    `json:"event"`
	At      time.Time `json:"at"`
	Target  struct {
		Namespace string `json:"namespace"`
		Pod       string `json:"pod"`
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		Proxy     string `json:"proxy"`
	} `json:"target"`
	Expects   []string `json:"expects"`
	SettleSec float64  `json:"settleSec"`
	// RestartBudget is how many §8.8 restart-or-replacement units this
	// injection legitimately produces (doc/checker.md); 0 in old logs
	// means 1.
	RestartBudget int `json:"restartBudget"`
}

// allowWindow is one injection's expected-effects interval for one signal.
type allowWindow struct {
	faultID string
	pod     string    // the target pod, for restart / scrape-gap scoping
	from    time.Time // the injection's started event
	to      time.Time // close event + settle; zero while the fault is active
	// budget is the injection's §8.8 unit allowance (restarts signal only).
	budget int
}

func (w allowWindow) contains(t time.Time) bool {
	if t.Before(w.from) {
		return false
	}
	return w.to.IsZero() || !t.After(w.to)
}

// faultState is the parsed allowance set of the current fault log.
type faultState struct {
	path    string
	windows map[string][]allowWindow // signal -> windows, in started order
}

func newFaultState(path string) *faultState {
	return &faultState{path: path, windows: map[string][]allowWindow{}}
}

// reload re-parses the fault log. Runs on every tick, strictly before
// evaluate() (doc/checker.md); a missing file just means no injection ran
// yet, and a torn last line is skipped and re-read next tick.
func (f *faultState) reload() error {
	file, err := os.Open(f.path)
	if os.IsNotExist(err) {
		f.windows = map[string][]allowWindow{}
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	type injection struct {
		started time.Time
		closed  time.Time
		settle  time.Duration
		expects []string
		pod     string
		budget  int
	}
	order := []string{}
	byID := map[string]*injection{}
	sc := bufio.NewScanner(file)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		var ev faultLogEvent
		// A torn last line fails to parse: skip it, the writer appends the
		// rest before the next tick.
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			continue
		}
		inj, ok := byID[ev.FaultID]
		if !ok {
			inj = &injection{}
			byID[ev.FaultID] = inj
			order = append(order, ev.FaultID)
		}
		inj.settle = time.Duration(ev.SettleSec * float64(time.Second))
		inj.expects = ev.Expects
		inj.pod = ev.Target.Pod
		inj.budget = ev.RestartBudget
		switch ev.Event {
		case "started":
			inj.started = ev.At
		case "ended", "reverted":
			// pod-delete emits ended; stateful actions emit reverted. The
			// later of the two closes the window.
			if ev.At.After(inj.closed) {
				inj.closed = ev.At
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}

	windows := map[string][]allowWindow{}
	for _, id := range order {
		inj := byID[id]
		if inj.started.IsZero() {
			continue // scheduled but not executed yet
		}
		budget := inj.budget
		if budget < 1 {
			budget = 1 // logs from before the field existed
		}
		w := allowWindow{faultID: id, pod: inj.pod, from: inj.started, budget: budget}
		if !inj.closed.IsZero() {
			w.to = inj.closed.Add(inj.settle)
		}
		for _, sig := range inj.expects {
			windows[sig] = append(windows[sig], w)
		}
	}
	f.windows = windows
	return nil
}

// expected reports whether an observation at t falls into any window of the
// signal.
func (f *faultState) expected(signal string, t time.Time) bool {
	for _, w := range f.windows[signal] {
		if w.contains(t) {
			return true
		}
	}
	return false
}

// expectedForPod scopes the check to windows whose injection targeted the
// named pod.
func (f *faultState) expectedForPod(signal, pod string, t time.Time) bool {
	for _, w := range f.windows[signal] {
		if w.pod == pod && w.contains(t) {
			return true
		}
	}
	return false
}

// overlapsOpen reports whether any window of the signal is open, or overlaps
// [from, to] — the §8.1 trend-skip rule.
func (f *faultState) overlaps(signal string, from, to time.Time) bool {
	for _, w := range f.windows[signal] {
		if w.to.IsZero() {
			if !w.from.After(to) {
				return true
			}
			continue
		}
		if !w.from.After(to) && !w.to.Before(from) {
			return true
		}
	}
	return false
}

// deadlineShift is the §8.5 rule: closed windows of the signal push a
// compaction deadline out by their full length (settle included); an open
// window is handled by the caller (the group is not judged at all).
func (f *faultState) deadlineShift(signal string, bucketEnd time.Time) time.Duration {
	var shift time.Duration
	for _, w := range f.windows[signal] {
		if w.to.IsZero() {
			continue
		}
		if w.to.After(bucketEnd) {
			shift += w.to.Sub(w.from)
		}
	}
	return shift
}

// hasOpen reports an active (unreverted) injection declaring the signal.
func (f *faultState) hasOpen(signal string) bool {
	for _, w := range f.windows[signal] {
		if w.to.IsZero() {
			return true
		}
	}
	return false
}

// restartAllowances lists the windows of the restarts signal for one pod, in
// injection order — each grants a budget of one restart-or-replacement event
// (doc/checker.md).
func (f *faultState) restartAllowances(pod string) []allowWindow {
	var out []allowWindow
	for _, w := range f.windows["restarts"] {
		if w.pod == pod {
			out = append(out, w)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].from.Before(out[j].from) })
	return out
}
