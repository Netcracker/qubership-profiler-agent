package rules

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// WatchOptions configures a Watcher.
type WatchOptions struct {
	Path        string
	Surface     Surface
	HasUpstream bool
	// Debounce groups the burst of events one file update produces.
	// Zero means 200ms.
	Debounce time.Duration
	// OnLoad receives every successfully parsed rule set, the first one
	// included. skipped counts rules that set a key Surface cannot
	// evaluate.
	OnLoad func(set Set, skipped int)
	// OnError receives every load failure. The rules from the last successful
	// load stay active; before the first one, there are none.
	//
	// The callbacks for the first load run inside Watch, before it returns.
	// Later ones run on the watcher goroutine, one at a time. Close waits for
	// that goroutine, so a callback that calls Close never returns: call
	// Close from outside the callbacks.
	OnError func(err error)
}

// Watcher reloads a rules file when it changes.
//
// It watches the file's directory, not the file: Kubernetes updates a
// ConfigMap mount by swapping a symlink, which a watch on the file itself
// would miss. Every event in the directory triggers a debounced re-read, and
// a load happens only when the file's content changed.
type Watcher struct {
	opts    WatchOptions
	fsw     *fsnotify.Watcher
	errs    <-chan error
	done    chan struct{}
	closed  sync.Once
	wg      sync.WaitGroup
	last    []byte
	hasLast bool
}

// Watch loads the file once and starts watching it. A failed first load is
// reported through OnError and does not stop the watch, so a fixed file
// takes effect without a restart.
func Watch(opts WatchOptions) (*Watcher, error) {
	w, err := newWatcher(opts)
	if err != nil {
		return nil, err
	}
	w.start()
	return w, nil
}

// newWatcher adds the directory watch but does not load the file or start
// the loop, so a test can replace errs before start.
func newWatcher(opts WatchOptions) (*Watcher, error) {
	if opts.Debounce == 0 {
		opts.Debounce = 200 * time.Millisecond
	}
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create rules file watcher: %w", err)
	}
	dir := filepath.Dir(opts.Path)
	if err := fsw.Add(dir); err != nil {
		_ = fsw.Close()
		return nil, fmt.Errorf("watch rules directory %s: %w", dir, err)
	}
	return &Watcher{opts: opts, fsw: fsw, errs: fsw.Errors, done: make(chan struct{})}, nil
}

func (w *Watcher) start() {
	w.reload()
	w.wg.Add(1)
	go w.loop()
}

func (w *Watcher) loop() {
	defer w.wg.Done()
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		select {
		case <-w.done:
			timer.Stop()
			return
		case _, ok := <-w.fsw.Events:
			if !ok {
				return
			}
			timer.Reset(w.opts.Debounce)
		case err, ok := <-w.errs:
			if !ok {
				return
			}
			w.fail(fmt.Errorf("watch rules file %s: %w", w.opts.Path, err))
			// An error such as ErrEventOverflow means events were lost, so
			// re-read the file instead of waiting for the next event.
			timer.Reset(w.opts.Debounce)
		case <-timer.C:
			w.reload()
		}
	}
}

func (w *Watcher) reload() {
	data, err := os.ReadFile(w.opts.Path)
	if err != nil {
		w.fail(fmt.Errorf("read rules file: %w", err))
		return
	}
	if w.hasLast && bytes.Equal(data, w.last) {
		return
	}
	set, skipped, err := ParseFile(data, w.opts.Surface, w.opts.HasUpstream)
	if err != nil {
		w.fail(fmt.Errorf("rules file %s: %w", w.opts.Path, err))
		return
	}
	w.last, w.hasLast = data, true
	if w.opts.OnLoad != nil {
		w.opts.OnLoad(set, skipped)
	}
}

func (w *Watcher) fail(err error) {
	// Forget the last good content, so restoring it counts as a change.
	w.hasLast = false
	if w.opts.OnError != nil {
		w.opts.OnError(err)
	}
}

// Close stops the watch. It is safe to call more than once.
func (w *Watcher) Close() error {
	var err error
	w.closed.Do(func() {
		close(w.done)
		err = w.fsw.Close()
		w.wg.Wait()
	})
	return err
}
