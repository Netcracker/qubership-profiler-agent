// Package budget is the process-wide read-memory budget of
// 02-read-contract.md §7.5: one Budget per query process, drawn from by every
// reader that materializes decoded bytes (cold parquet batches and pages, hot
// fan-out bodies, hot trace blobs). Callers charge BEFORE materializing and
// release when the memory is handed off or dropped, so the budget bounds what
// the readers hold, not what the runtime happens to report.
//
// Queueing semantics, deliberately asymmetric:
//
//   - Acquire is a strict-FIFO admission: a new unit of work waits its turn
//     and is granted only at the queue head, so a wide request cannot be
//     starved by a stream of small ones.
//   - Grow serves a lease that already holds capacity and therefore blocks
//     everyone behind it; it bypasses the admission queue and is served with
//     priority over the admission head. The FIFO fairness guarantee covers
//     admissions only — a burst of Grows can delay new Acquires, which is the
//     accepted trade (§7.5).
//
// Two Grows (or a Grow and a queued Acquire) can mutually block when the
// budget is exhausted by their holders; the bounded wait converts that into
// ErrExhausted for the loser rather than a deadlock. The resulting 503 can
// fire while the budget is formally sufficient — an accepted outcome, bounded
// by the small Grow increments the readers use.
package budget

import (
	"container/list"
	"context"
	"time"

	"github.com/pkg/errors"
)

// ErrExhausted is a transient denial: the bounded wait elapsed with the
// budget still held by others. The caller answers 503 with Retry-After.
var ErrExhausted = errors.New("read memory budget exhausted")

// ErrNeverFits is a structural denial: the requested bytes plus what the
// lease already holds exceed the whole budget, so no amount of waiting can
// help. Distinguished from ErrExhausted in metrics and problem details.
var ErrNeverFits = errors.New("request exceeds the whole read memory budget")

// Hooks receives accounting updates for metrics. Callbacks run outside the
// budget lock; a nil hook is skipped.
type Hooks struct {
	// OnUsed is called with the new total of charged bytes after every change.
	OnUsed func(used int64)
}

type waiter struct {
	n       int64
	ready   chan struct{}
	granted bool
}

// Budget is the process-wide byte budget. The zero value is not usable; call
// New. A nil *Budget is a valid no-op budget: every operation succeeds and
// accounts nothing (tests, budget-disabled embedders).
type Budget struct {
	limit int64
	wait  time.Duration
	hooks Hooks

	mu    chan struct{} // 1-slot semaphore as mutex; keeps grant bookkeeping simple
	used  int64
	admit *list.List // *waiter, strict FIFO, granted at head only
	grow  *list.List // *waiter, served before the admission head
}

// New builds a budget of limit bytes with the given queue wait. limit must be
// positive; wait <= 0 disables queueing (an unsatisfiable charge fails
// immediately with ErrExhausted).
func New(limit int64, wait time.Duration, hooks Hooks) *Budget {
	b := &Budget{
		limit: limit,
		wait:  wait,
		hooks: hooks,
		mu:    make(chan struct{}, 1),
		admit: list.New(),
		grow:  list.New(),
	}
	return b
}

// Lease is one owner's slice of the budget. The zero value and a lease from a
// nil budget are no-op leases. A Lease is not safe for concurrent use.
type Lease struct {
	b    *Budget
	held int64
}

// Used reports the currently charged total, for tests and gauges.
func (b *Budget) Used() int64 {
	if b == nil {
		return 0
	}
	b.lock()
	defer b.unlock()
	return b.used
}

// Limit reports the configured budget size (0 for a nil budget).
func (b *Budget) Limit() int64 {
	if b == nil {
		return 0
	}
	return b.limit
}

// NewLease returns an empty lease that bypasses the queue; capacity enters it
// via TransferTo (and Grow). The request-scoped page lease of §7.5 is one.
func (b *Budget) NewLease() *Lease {
	return &Lease{b: b}
}

// Acquire charges n bytes as a new unit of work: immediate when the budget is
// free and nobody queues ahead, otherwise a strict-FIFO wait bounded by the
// budget's wait. Returns ErrNeverFits when n alone exceeds the whole budget,
// ErrExhausted when the wait elapses, or ctx.Err() when the caller gives up.
func (b *Budget) Acquire(ctx context.Context, n int64) (*Lease, error) {
	if b == nil || n <= 0 {
		return &Lease{b: b}, nil
	}
	if n > b.limit {
		return nil, errors.Wrapf(ErrNeverFits, "acquire %d of %d", n, b.limit)
	}
	b.lock()
	// Immediate grant only when nobody is ahead: pending admissions keep FIFO
	// order, pending grows hold the priority lane.
	if b.admit.Len() == 0 && b.grow.Len() == 0 && b.used+n <= b.limit {
		b.used += n
		used := b.used
		b.unlock()
		b.notifyUsed(used)
		return &Lease{b: b, held: n}, nil
	}
	w := &waiter{n: n, ready: make(chan struct{})}
	elem := b.admit.PushBack(w)
	b.unlock()
	if err := b.await(ctx, w, elem, b.admit); err != nil {
		return nil, err
	}
	return &Lease{b: b, held: n}, nil
}

// Grow charges n more bytes onto an existing lease, out of the admission
// FIFO: free capacity is taken immediately, otherwise the grow waits with
// priority over the admission head (see the package comment for why).
func (l *Lease) Grow(ctx context.Context, n int64) error {
	if l == nil || l.b == nil || n <= 0 {
		return nil
	}
	b := l.b
	if l.held+n > b.limit {
		return errors.Wrapf(ErrNeverFits, "grow %d over %d held of %d", n, l.held, b.limit)
	}
	b.lock()
	// Immediate only when no grow queues ahead: grows stay FIFO among
	// themselves even though they overtake admissions.
	if b.grow.Len() == 0 && b.used+n <= b.limit {
		b.used += n
		used := b.used
		b.unlock()
		b.notifyUsed(used)
		l.held += n
		return nil
	}
	w := &waiter{n: n, ready: make(chan struct{})}
	elem := b.grow.PushBack(w)
	b.unlock()
	if err := b.await(ctx, w, elem, b.grow); err != nil {
		return err
	}
	l.held += n
	return nil
}

// Shrink returns n bytes early (an estimate reconciled down). Shrinking below
// zero held is a bug; the excess is clamped to keep accounting sane.
func (l *Lease) Shrink(n int64) {
	if l == nil || l.b == nil || n <= 0 {
		return
	}
	if n > l.held {
		n = l.held
	}
	l.held -= n
	l.b.credit(n)
}

// TransferTo moves n bytes of ownership into dst (the request page lease or
// the point lease). No budget capacity changes hands with the queue — the
// bytes stay charged. n above the held amount is clamped.
func (l *Lease) TransferTo(dst *Lease, n int64) {
	if l == nil || l.b == nil || dst == nil || n <= 0 {
		return
	}
	if n > l.held {
		n = l.held
	}
	l.held -= n
	dst.held += n
	if dst.b == nil {
		// A no-op destination cannot release later; return the bytes now
		// rather than leak them.
		dst.held -= n
		l.b.credit(n)
	}
}

// Held reports the bytes this lease currently owns.
func (l *Lease) Held() int64 {
	if l == nil {
		return 0
	}
	return l.held
}

// Release returns everything the lease still holds. Safe to call more than
// once and on no-op leases, so `defer lease.Release()` right after every
// Acquire is always correct.
func (l *Lease) Release() {
	if l == nil || l.b == nil || l.held == 0 {
		return
	}
	n := l.held
	l.held = 0
	l.b.credit(n)
}

// credit returns n bytes to the pool and grants what the freed capacity
// covers: grow waiters first (in their own arrival order), then the admission
// head only — strict FIFO for admissions.
func (b *Budget) credit(n int64) {
	b.lock()
	b.used -= n
	var ready []chan struct{}
	for e := b.grow.Front(); e != nil; {
		w := e.Value.(*waiter)
		if b.used+w.n > b.limit {
			break
		}
		next := e.Next()
		b.grow.Remove(e)
		b.used += w.n
		w.granted = true
		ready = append(ready, w.ready)
		e = next
	}
	for b.grow.Len() == 0 {
		e := b.admit.Front()
		if e == nil {
			break
		}
		w := e.Value.(*waiter)
		if b.used+w.n > b.limit {
			break
		}
		b.admit.Remove(e)
		b.used += w.n
		w.granted = true
		ready = append(ready, w.ready)
	}
	used := b.used
	b.unlock()
	for _, ch := range ready {
		close(ch)
	}
	b.notifyUsed(used)
}

// await blocks on a queued waiter until grant, timeout, or ctx cancellation.
// A cancelled or timed-out waiter removes itself from the queue whatever its
// position — a dead non-head waiter must not clog the FIFO.
func (b *Budget) await(ctx context.Context, w *waiter, elem *list.Element, q *list.List) error {
	timer := time.NewTimer(b.wait)
	defer timer.Stop()
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		return b.abandon(w, elem, q, ctx.Err())
	case <-timer.C:
		return b.abandon(w, elem, q, errors.Wrapf(ErrExhausted, "after %s", b.wait))
	}
}

// abandon resolves the race between a losing waiter and a concurrent grant:
// if the grant happened first, the charge is honored and returned; otherwise
// the waiter leaves the queue.
func (b *Budget) abandon(w *waiter, elem *list.Element, q *list.List, cause error) error {
	b.lock()
	if w.granted {
		b.unlock()
		// The grant beat the timeout; the caller proceeds as granted.
		return nil
	}
	q.Remove(elem)
	b.unlock()
	return cause
}

func (b *Budget) lock()   { b.mu <- struct{}{} }
func (b *Budget) unlock() { <-b.mu }

func (b *Budget) notifyUsed(used int64) {
	if b.hooks.OnUsed != nil {
		b.hooks.OnUsed(used)
	}
}
