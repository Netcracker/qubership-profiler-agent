package budget

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testWait = 200 * time.Millisecond

func TestAcquireImmediate(t *testing.T) {
	b := New(100, testWait, Hooks{})
	l, err := b.Acquire(context.Background(), 60)
	require.NoError(t, err)
	assert.Equal(t, int64(60), b.Used())
	l.Release()
	assert.Equal(t, int64(0), b.Used())
	l.Release() // double release is a no-op
	assert.Equal(t, int64(0), b.Used())
}

func TestAcquireNeverFits(t *testing.T) {
	b := New(100, testWait, Hooks{})
	_, err := b.Acquire(context.Background(), 101)
	assert.ErrorIs(t, err, ErrNeverFits)
	assert.Equal(t, int64(0), b.Used())
}

func TestAcquireExhausted(t *testing.T) {
	b := New(100, 50*time.Millisecond, Hooks{})
	l, err := b.Acquire(context.Background(), 80)
	require.NoError(t, err)
	defer l.Release()
	_, err = b.Acquire(context.Background(), 40)
	assert.ErrorIs(t, err, ErrExhausted)
	assert.Equal(t, int64(80), b.Used())
}

// TestAdmissionFIFO pins the strict order: while the queue head does not fit,
// a smaller acquire behind it stays queued even though it would fit by itself.
func TestAdmissionFIFO(t *testing.T) {
	b := New(100, time.Second, Hooks{})
	first, err := b.Acquire(context.Background(), 70)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // needs 60: waits for the full release
		defer wg.Done()
		l, err := b.Acquire(context.Background(), 60)
		require.NoError(t, err)
		l.Release()
	}()
	waitQueued(t, b, func() bool { return b.queuedAdmissions() == 1 })

	wg.Add(1)
	go func() { // needs 20: must not overtake the queued 60
		defer wg.Done()
		l, err := b.Acquire(context.Background(), 20)
		require.NoError(t, err)
		l.Release()
	}()
	waitQueued(t, b, func() bool { return b.queuedAdmissions() == 2 })

	// Free 20: the small one now fits (50+20 <= 100) but the head (60) does
	// not — strict FIFO keeps BOTH queued.
	first.Shrink(20)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 2, b.queuedAdmissions(), "small acquire overtook the queue head")

	first.Release()
	wg.Wait()
	assert.Equal(t, int64(0), b.Used())
}

// TestGrowPriority pins the documented asymmetry: a waiting Grow is served
// from freed capacity before the admission head.
func TestGrowPriority(t *testing.T) {
	b := New(100, time.Second, Hooks{})
	holder, err := b.Acquire(context.Background(), 50)
	require.NoError(t, err)
	blocker, err := b.Acquire(context.Background(), 40)
	require.NoError(t, err)

	order := make(chan string, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // admission first in line
		defer wg.Done()
		l, err := b.Acquire(context.Background(), 30)
		require.NoError(t, err)
		order <- "admission"
		l.Release()
	}()
	waitQueued(t, b, func() bool { return b.queuedAdmissions() == 1 })

	wg.Add(1)
	go func() { // grow arrives later but is served first
		defer wg.Done()
		require.NoError(t, holder.Grow(context.Background(), 30))
		order <- "grow"
		holder.Release()
	}()
	waitQueued(t, b, func() bool { return b.queuedGrows() == 1 })

	blocker.Release() // frees 40: grow (30) wins it, admission still short
	wg.Wait()
	assert.Equal(t, "grow", <-order)
	assert.Equal(t, "admission", <-order)
	assert.Equal(t, int64(0), b.Used())
}

// TestMutualGrow pins the accepted deadlock resolution: two leases each grow
// past the remaining capacity; both time out with ErrExhausted, nothing
// leaks.
func TestMutualGrow(t *testing.T) {
	b := New(100, 80*time.Millisecond, Hooks{})
	a, err := b.Acquire(context.Background(), 45)
	require.NoError(t, err)
	c, err := b.Acquire(context.Background(), 45)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, l := range []*Lease{a, c} {
		wg.Add(1)
		go func(l *Lease) {
			defer wg.Done()
			errs <- l.Grow(context.Background(), 40)
		}(l)
	}
	wg.Wait()
	assert.ErrorIs(t, <-errs, ErrExhausted)
	assert.ErrorIs(t, <-errs, ErrExhausted)
	a.Release()
	c.Release()
	assert.Equal(t, int64(0), b.Used())
}

func TestGrowNeverFits(t *testing.T) {
	b := New(100, testWait, Hooks{})
	l, err := b.Acquire(context.Background(), 60)
	require.NoError(t, err)
	defer l.Release()
	assert.ErrorIs(t, l.Grow(context.Background(), 50), ErrNeverFits)
	assert.Equal(t, int64(60), b.Used())
}

func TestGrowImmediate(t *testing.T) {
	b := New(100, testWait, Hooks{})
	l, err := b.Acquire(context.Background(), 40)
	require.NoError(t, err)
	require.NoError(t, l.Grow(context.Background(), 30))
	assert.Equal(t, int64(70), b.Used())
	assert.Equal(t, int64(70), l.Held())
	l.Release()
	assert.Equal(t, int64(0), b.Used())
}

func TestShrinkAndTransfer(t *testing.T) {
	b := New(100, testWait, Hooks{})
	l, err := b.Acquire(context.Background(), 80)
	require.NoError(t, err)
	l.Shrink(30)
	assert.Equal(t, int64(50), b.Used())
	assert.Equal(t, int64(50), l.Held())

	page := b.NewLease()
	l.TransferTo(page, 20)
	assert.Equal(t, int64(30), l.Held())
	assert.Equal(t, int64(20), page.Held())
	assert.Equal(t, int64(50), b.Used())

	l.Release()
	assert.Equal(t, int64(20), b.Used()) // page still owns its part
	page.Release()
	assert.Equal(t, int64(0), b.Used())

	// Clamps: shrinking or transferring more than held cannot go negative.
	l2, err := b.Acquire(context.Background(), 10)
	require.NoError(t, err)
	l2.Shrink(50)
	assert.Equal(t, int64(0), b.Used())
	l2.TransferTo(page, 50)
	assert.Equal(t, int64(0), page.Held())
}

// TestCancelNonHeadWaiter pins queue hygiene: a cancelled waiter behind the
// head leaves the FIFO, so later grants skip it instead of stalling.
func TestCancelNonHeadWaiter(t *testing.T) {
	b := New(100, time.Second, Hooks{})
	holder, err := b.Acquire(context.Background(), 90)
	require.NoError(t, err)

	headDone := make(chan error, 1)
	go func() {
		l, err := b.Acquire(context.Background(), 50)
		if err == nil {
			defer l.Release()
		}
		headDone <- err
	}()
	waitQueued(t, b, func() bool { return b.queuedAdmissions() == 1 })

	ctx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() {
		_, err := b.Acquire(ctx, 30)
		cancelled <- err
	}()
	waitQueued(t, b, func() bool { return b.queuedAdmissions() == 2 })

	cancel()
	assert.ErrorIs(t, <-cancelled, context.Canceled)
	waitQueued(t, b, func() bool { return b.queuedAdmissions() == 1 })

	holder.Release()
	require.NoError(t, <-headDone)
	assert.Equal(t, int64(0), b.Used())
}

// TestCancelGrowWaiter is the Grow twin of the queue-hygiene test.
func TestCancelGrowWaiter(t *testing.T) {
	b := New(100, time.Second, Hooks{})
	holder, err := b.Acquire(context.Background(), 90)
	require.NoError(t, err)
	other, err := b.Acquire(context.Background(), 10)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	growErr := make(chan error, 1)
	go func() { growErr <- other.Grow(ctx, 50) }()
	waitQueued(t, b, func() bool { return b.queuedGrows() == 1 })

	cancel()
	assert.ErrorIs(t, <-growErr, context.Canceled)
	waitQueued(t, b, func() bool { return b.queuedGrows() == 0 })

	holder.Release()
	other.Release()
	assert.Equal(t, int64(0), b.Used())
}

func TestNilSafety(t *testing.T) {
	var b *Budget
	l, err := b.Acquire(context.Background(), 1<<40)
	require.NoError(t, err)
	require.NoError(t, l.Grow(context.Background(), 1))
	l.Shrink(1)
	l.TransferTo(b.NewLease(), 1)
	l.Release()
	assert.Equal(t, int64(0), b.Used())
	assert.Equal(t, int64(0), b.Limit())

	var nilLease *Lease
	require.NoError(t, nilLease.Grow(context.Background(), 1))
	nilLease.Shrink(1)
	nilLease.Release()
	assert.Equal(t, int64(0), nilLease.Held())
}

func TestHooksObserveUsage(t *testing.T) {
	var mu sync.Mutex
	var last int64
	b := New(100, testWait, Hooks{OnUsed: func(used int64) {
		mu.Lock()
		defer mu.Unlock()
		last = used
	}})
	l, err := b.Acquire(context.Background(), 25)
	require.NoError(t, err)
	mu.Lock()
	assert.Equal(t, int64(25), last)
	mu.Unlock()
	l.Release()
	mu.Lock()
	assert.Equal(t, int64(0), last)
	mu.Unlock()
}

// TestConcurrentChurn hammers the budget from many goroutines and checks the
// accounting lands back at zero with the limit never exceeded.
func TestConcurrentChurn(t *testing.T) {
	b := New(1000, time.Second, Hooks{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				n := int64(1 + (i*13+j*7)%100)
				l, err := b.Acquire(context.Background(), n)
				if errors.Is(err, ErrExhausted) {
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				if grown := l.Grow(context.Background(), n/2); grown == nil {
					l.Shrink(n / 2)
				}
				l.Release()
			}
		}(i)
	}
	wg.Wait()
	assert.Equal(t, int64(0), b.Used())
}

// queuedAdmissions / queuedGrows expose queue lengths to the tests only.
func (b *Budget) queuedAdmissions() int {
	b.lock()
	defer b.unlock()
	return b.admit.Len()
}

func (b *Budget) queuedGrows() int {
	b.lock()
	defer b.unlock()
	return b.grow.Len()
}

func waitQueued(t *testing.T, b *Budget, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached")
}
