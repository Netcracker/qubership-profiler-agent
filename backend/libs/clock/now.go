package clock

import (
	"sync"
	"time"
)

var (
	isTest  = false
	curTime time.Time    // will be overridden by `clock.As()` for test purposes
	mux     sync.RWMutex // clock should be overridden only by one test in parallel
)

func Now() time.Time {
	if !isTest {
		return time.Now()
	}
	return curTime
}

func Since(t1 time.Time) time.Duration {
	if !isTest {
		return time.Since(t1)
	}
	return t1.Sub(curTime)
}

func As(t time.Time, f func()) {
	mux.Lock()
	defer mux.Unlock()
	isTest = true
	curTime = t
	f()
	isTest = false
	curTime = time.Now() // should not be used when `isTest=false`
}
