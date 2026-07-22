// Stand lock: one run per stand as a technical guarantee, not a convention
// (doc/run-orchestration.md, "Stand lock"). A lease file under runs/ names
// the running testid; a live foreign lease refuses the start, and the
// preflight fault recovery never touches state owned by a live lease.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	leaseTTL           = 90 * time.Second
	leaseRenewInterval = 30 * time.Second
)

type leaseFile struct {
	TestID     string    `json:"testid"`
	PID        int       `json:"pid"`
	AcquiredAt time.Time `json:"acquiredAt"`
	RenewedAt  time.Time `json:"renewedAt"`
	TTLSec     float64   `json:"ttlSec"`
}

func (l leaseFile) live(now time.Time) bool {
	return now.Before(l.RenewedAt.Add(time.Duration(l.TTLSec * float64(time.Second))))
}

type standLock struct {
	path   string
	testid string
	cancel context.CancelFunc
}

func lockPath(outputs string) string { return filepath.Join(outputs, ".stand-lock") }

// readLease returns the current lease, or nil when none exists.
func readLease(outputs string) (*leaseFile, error) {
	raw, err := os.ReadFile(lockPath(outputs))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var l leaseFile
	if err := json.Unmarshal(raw, &l); err != nil {
		return nil, fmt.Errorf("%s: %w", lockPath(outputs), err)
	}
	return &l, nil
}

// acquireStandLock takes the lease and renews it in the background until
// release. A live foreign lease is a refusal — the caller must not proceed
// and must not run fault recovery.
func acquireStandLock(ctx context.Context, outputs, testid string) (*standLock, error) {
	if err := os.MkdirAll(outputs, 0o755); err != nil {
		return nil, err
	}
	cur, err := readLease(outputs)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	if cur != nil && cur.live(now) && cur.TestID != testid {
		return nil, fmt.Errorf("stand is locked by run %q (pid %d, renewed %s): one run per stand — wait for it or investigate its lease at %s",
			cur.TestID, cur.PID, cur.RenewedAt.Format(time.RFC3339), lockPath(outputs))
	}
	l := &standLock{path: lockPath(outputs), testid: testid}
	if err := l.write(now, now); err != nil {
		return nil, err
	}
	renewCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	go func() {
		acquired := now
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-time.After(leaseRenewInterval):
				if err := l.write(acquired, time.Now()); err != nil {
					fmt.Fprintf(os.Stderr, "runner: lease renewal: %v\n", err)
				}
			}
		}
	}()
	return l, nil
}

func (l *standLock) write(acquired, renewed time.Time) error {
	raw, err := json.MarshalIndent(leaseFile{
		TestID: l.testid, PID: os.Getpid(),
		AcquiredAt: acquired, RenewedAt: renewed, TTLSec: leaseTTL.Seconds(),
	}, "", "  ")
	if err != nil {
		return err
	}
	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, l.path)
}

// release stops the renewal and removes the lease.
func (l *standLock) release() {
	if l.cancel != nil {
		l.cancel()
	}
	_ = os.Remove(l.path)
}
