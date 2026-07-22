package main

import (
	"fmt"
	"time"
)

// finding is one invariant failure at one evaluation tick, tied to the
// subject it judged (a target URL, an S3 prefix, a marker PK, a pod name).
// observedAt is the time of the observation that produced the evidence (the
// scrape, listing, probe, or pod list) — allowance windows match against it,
// never against the evaluation time. expected marks a finding that falls
// into a declared fault window (doc/checker.md, "Expected failures").
type finding struct {
	subject    string
	msg        string
	observedAt time.Time
	expected   bool
}

// violationRecord is one latched violation: a (invariant, subject) pair that
// failed at least once after warm-up. It never clears (doc/checker.md).
type violationRecord struct {
	invariant string
	plan      string
	subject   string
	expected  bool
	firstAt   time.Time
	lastAt    time.Time
	count     int
	lastMsg   string
}

// latch accumulates violations for the whole run; the exit code and the
// final report come from here, not from the last evaluation pass. Expected
// and unexpected records latch separately: only unexpected ones fail the
// run, but both are evidence.
type latch struct {
	recs  map[string]*violationRecord
	order []string
}

func newLatch() *latch {
	return &latch{recs: map[string]*violationRecord{}}
}

// record latches one finding. It returns the record and whether it is new,
// so the caller can print new violations louder than repeats.
func (l *latch) record(at time.Time, inv invariant, f finding) (*violationRecord, bool) {
	key := inv.name + "|" + f.subject
	if f.expected {
		key += "|expected"
	}
	if rec, ok := l.recs[key]; ok {
		rec.lastAt = at
		rec.count++
		rec.lastMsg = f.msg
		return rec, false
	}
	rec := &violationRecord{
		invariant: inv.name, plan: inv.plan, subject: f.subject, expected: f.expected,
		firstAt: at, lastAt: at, count: 1, lastMsg: f.msg,
	}
	l.recs[key] = rec
	l.order = append(l.order, key)
	return rec, true
}

// unexpectedLen counts the records that fail the run.
func (l *latch) unexpectedLen() int {
	n := 0
	for _, rec := range l.recs {
		if !rec.expected {
			n++
		}
	}
	return n
}

// expectedLen counts the allowance-matched records.
func (l *latch) expectedLen() int { return len(l.recs) - l.unexpectedLen() }

// report prints every latched violation in first-seen order: unexpected
// records first, expected ones under their own heading.
func (l *latch) report() {
	l.printGroup("VIOLATION", false)
	if l.expectedLen() > 0 {
		fmt.Println("expected (allowance-matched) violations:")
		l.printGroup("EXPECTED", true)
	}
}

func (l *latch) printGroup(label string, expected bool) {
	for _, key := range l.order {
		rec := l.recs[key]
		if rec.expected != expected {
			continue
		}
		fmt.Printf("%s %s (%s) %s: %s [first %s, last %s, %d times]\n",
			label, rec.invariant, rec.plan, rec.subject, rec.lastMsg,
			rec.firstAt.Format(time.RFC3339), rec.lastAt.Format(time.RFC3339), rec.count)
	}
}
