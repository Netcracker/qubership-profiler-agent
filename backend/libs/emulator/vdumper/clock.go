package vdumper

import "time"

// Clock abstracts wall-clock reads and timer waits so lifecycle tests and
// accelerated runs can drive the dumper timers themselves.
type Clock interface {
	Now() time.Time
	// After behaves like time.After; a zero or negative duration fires
	// immediately.
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock returns the wall clock.
func RealClock() Clock { return realClock{} }
