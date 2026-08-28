package vdumper

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Well-known dictionary positions: the first words are the param names the
// workload tags calls with, so their ids are stable. The collector resolves
// the `call.red` error marker by dictionary word (hotstore's
// errorMarkerParam), which is why it must be present. The big-param tag ids
// (`sql`, `xml`) name the value streams their tag events reference; the
// BIG/BIG_DEDUP nature travels in the trace event's param-type byte, not in
// the params stream.
const (
	dictRequestId = 0 // "request.id"
	dictCallRed   = 1 // "call.red"
	dictSql       = 2 // "sql"
	dictXml       = 3 // "xml"
	// The dumper injects these tags into the trace on every recorded call
	// (Dumper.writeBufferToFS + writeCallParams).
	dictCommonStarted = 4 // "common.started"
	dictNodeName      = 5 // "node.name"
	dictJavaThread    = 6 // "java.thread"
	dictTimeCpu       = 7 // "time.cpu"
	dictTimeWait      = 8 // "time.wait"
	dictMemAllocated  = 9 // "memory.allocated"

	dictMethods = 10 // first synthetic method word
)

// dictionary models ProfilerData's tag list: an append-only word list whose
// ids are implied by position. It grows over time (DictionaryGrowthPerMin)
// and is read by producer goroutines picking method ids, so it carries a
// mutex. The sent counter is per-connection — every reconnect resets it to
// zero so the whole dictionary is re-sent (Dumper.initialize), which is what
// resetRequired=1 tells the collector.
type dictionary struct {
	mu    sync.Mutex
	words []string
	sent  int

	growthPerMin float64
	grownAt      time.Time // last growth accounting instant
	backlog      float64   // fractional words owed by growth
}

// newDictionary builds the startup tag list: the well-known param words plus
// synthetic methods up to total words. Total is clamped to the well-known
// prefix plus one method.
func newDictionary(total int, growthPerMin float64) *dictionary {
	d := &dictionary{growthPerMin: growthPerMin}
	d.words = append(d.words, "request.id", "call.red", "sql", "xml",
		"common.started", "node.name", "java.thread", "time.cpu", "time.wait", "memory.allocated")
	for len(d.words) < max(total, dictMethods+1) {
		d.words = append(d.words, syntheticWord(len(d.words)))
	}
	return d
}

// methodId picks a uniformly random method word id; the range widens as the
// dictionary grows.
func (d *dictionary) methodId(rnd *rand.Rand) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return dictMethods + rnd.Intn(len(d.words)-dictMethods)
}

// grow appends the words the growth rate owes for the elapsed wall time.
func (d *dictionary) grow(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.growthPerMin <= 0 {
		return
	}
	if d.grownAt.IsZero() {
		d.grownAt = now
		return
	}
	d.backlog += now.Sub(d.grownAt).Minutes() * d.growthPerMin
	d.grownAt = now
	for d.backlog >= 1 {
		d.words = append(d.words, syntheticWord(len(d.words)))
		d.backlog--
	}
}

// takePending returns the words not yet sent on this connection and marks
// them sent.
func (d *dictionary) takePending() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sent >= len(d.words) {
		return nil
	}
	pending := d.words[d.sent:]
	d.sent = len(d.words)
	return pending
}

// resetSent restarts the per-connection resend (Dumper.initialize).
func (d *dictionary) resetSent() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sent = 0
}

// sentNothing reports whether this connection has sent no dictionary words
// yet — the resetRequired condition of the dictionary stream open.
func (d *dictionary) sentNothing() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sent == 0
}

// syntheticWord shapes one dictionary entry like an instrumented-method tag:
// realistic length (~70 chars) matters for the bytes/s calibration, content
// does not.
func syntheticWord(i int) string {
	return fmt.Sprintf("void com.load.gen.Svc%04d.op%02d(int) (SyntheticApp.java) [synthetic-app.jar]",
		i/16, i%16)
}
