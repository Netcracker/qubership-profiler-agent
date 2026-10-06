package vdumper_test

import (
	"sync"
	"time"
)

// fakeClock drives the dumper timers by hand: After registers a waiter,
// Advance moves time and fires everything due. Tests wait for Waiters() to
// know the dumper is parked on a timer before advancing.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

type fakeTimer struct {
	at time.Time
	ch chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.timers = append(c.timers, fakeTimer{at: c.now.Add(d), ch: ch})
	return ch
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	remaining := c.timers[:0]
	for _, t := range c.timers {
		if !t.at.After(c.now) {
			t.ch <- c.now
			continue
		}
		remaining = append(remaining, t)
	}
	c.timers = remaining
}

// Waiters reports how many timers are armed — i.e. how many goroutines are
// parked on After.
func (c *fakeClock) Waiters() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}
