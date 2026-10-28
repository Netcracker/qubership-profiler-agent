package rules

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetEvalFirstMatchWins(t *testing.T) {
	s := Set{
		{Match: Match{Service: "chatty-*"}, Mode: Discard},
		{Match: Match{Service: "chatty-billing"}, Mode: Passthrough},
		{Match: Match{Namespace: "prod", DumpType: []string{DumpTypeHeap}}, Mode: Discard},
	}
	m, ok := s.Eval(Attrs{Service: "chatty-billing"})
	assert.True(t, ok)
	assert.Equal(t, Discard, m)

	m, ok = s.Eval(Attrs{Namespace: "prod", DumpType: DumpTypeHeap})
	assert.True(t, ok)
	assert.Equal(t, Discard, m)

	_, ok = s.Eval(Attrs{Namespace: "prod", DumpType: DumpTypeThread})
	assert.False(t, ok)

	_, ok = s.Eval(Attrs{})
	assert.False(t, ok, "a rule with a key does not match a source missing that attribute")
}

func TestEmptyMatchMatchesEverything(t *testing.T) {
	m, ok := Set{{Mode: Discard}}.Eval(Attrs{})
	assert.True(t, ok)
	assert.Equal(t, Discard, m)
}

func TestPolicyOrder(t *testing.T) {
	p := &Policy{Default: Passthrough, Inline: Set{{Match: Match{Pod: "a-*"}, Mode: Discard}}}
	assert.Equal(t, Discard, p.Eval(Attrs{Pod: "a-1"}))
	assert.Equal(t, Passthrough, p.Eval(Attrs{Pod: "b-1"}))

	p.SetFileRules(Set{{Match: Match{Pod: "a-1"}, Mode: Passthrough}})
	assert.Equal(t, Passthrough, p.Eval(Attrs{Pod: "a-1"}), "file rules go before inline rules")
	assert.Equal(t, Discard, p.Eval(Attrs{Pod: "a-2"}))
}

func TestValidate(t *testing.T) {
	ok := Set{{Match: Match{Namespace: "ns-*", DumpType: []string{"td", "gc"}}, Mode: Discard}}
	require.NoError(t, ok.Validate(Dumps, true))

	for name, tc := range map[string]struct {
		set         Set
		surface     Surface
		hasUpstream bool
	}{
		"unknown mode":            {Set{{Mode: "reject"}}, Dumps, true},
		"passthrough no upstream": {Set{{Mode: Passthrough}}, Dumps, false},
		"service on dumps":        {Set{{Match: Match{Service: "x"}, Mode: Discard}}, Dumps, true},
		"dump_type on tcp":        {Set{{Match: Match{DumpType: []string{"td"}}, Mode: Discard}}, TCP, true},
		"bad glob":                {Set{{Match: Match{Pod: "["}, Mode: Discard}}, TCP, true},
		"unknown dump type":       {Set{{Match: Match{DumpType: []string{"core"}}, Mode: Discard}}, Dumps, true},
	} {
		assert.Error(t, tc.set.Validate(tc.surface, tc.hasUpstream), name)
	}
}

const sharedFile = `
rules:
  - match: { service: "chatty-*" }
    mode: discard
  - match: { namespace: "ns-1", dump_type: [hprof] }
    mode: discard
  - match: { pod: "debug-*" }
    mode: passthrough
`

func TestParseFileSkipsTheOtherSurface(t *testing.T) {
	set, skipped, err := ParseFile([]byte(sharedFile), Dumps, true)
	require.NoError(t, err)
	assert.Equal(t, 1, skipped)
	assert.Len(t, set, 2)

	set, skipped, err = ParseFile([]byte(sharedFile), TCP, true)
	require.NoError(t, err)
	assert.Equal(t, 1, skipped)
	assert.Len(t, set, 2)
	assert.Equal(t, "chatty-*", set[0].Match.Service)
}

func TestParseFileErrors(t *testing.T) {
	for name, data := range map[string]string{
		"unknown key":  "rules:\n  - match: { host: x }\n    mode: discard\n",
		"unknown top":  "rule: []\n",
		"bad mode":     "rules:\n  - mode: drop\n",
		"not yaml":     "rules: [",
		"bad dumptype": "rules:\n  - match: { dump_type: [core] }\n    mode: discard\n",
	} {
		_, _, err := ParseFile([]byte(data), Dumps, true)
		assert.Error(t, err, name)
	}
}

func TestParseFileEmpty(t *testing.T) {
	set, skipped, err := ParseFile(nil, Dumps, true)
	require.NoError(t, err)
	assert.Empty(t, set)
	assert.Zero(t, skipped)
}

type recorder struct {
	mu     sync.Mutex
	loads  []Set
	errors []error
}

func (r *recorder) onLoad(s Set, _ int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loads = append(r.loads, s)
}

func (r *recorder) onError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, err)
}

func (r *recorder) counts() (loads, errs int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.loads), len(r.errors)
}

func (r *recorder) lastLoad() Set {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.loads[len(r.loads)-1]
}

func startWatch(t *testing.T, path string, rec *recorder) *Watcher {
	t.Helper()
	w, err := Watch(WatchOptions{
		Path: path, Surface: Dumps, HasUpstream: true, Debounce: 20 * time.Millisecond,
		OnLoad: rec.onLoad, OnError: rec.onError,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestWatcherReloadsOnWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rules.yaml")
	require.NoError(t, os.WriteFile(path, []byte("rules: []\n"), 0o600))
	rec := &recorder{}
	startWatch(t, path, rec)

	loads, _ := rec.counts()
	require.Equal(t, 1, loads, "the first load happens synchronously")

	require.NoError(t, os.WriteFile(path, []byte("rules:\n  - mode: discard\n"), 0o600))
	require.Eventually(t, func() bool { l, _ := rec.counts(); return l == 2 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, Discard, rec.lastLoad()[0].Mode)
}

func TestWatcherKeepsRulesOnBadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rules.yaml")
	require.NoError(t, os.WriteFile(path, []byte("rules:\n  - mode: discard\n"), 0o600))
	rec := &recorder{}
	startWatch(t, path, rec)

	require.NoError(t, os.WriteFile(path, []byte("rules: ["), 0o600))
	require.Eventually(t, func() bool { _, e := rec.counts(); return e >= 1 }, 5*time.Second, 10*time.Millisecond)
	loads, _ := rec.counts()
	assert.Equal(t, 1, loads, "a bad file does not replace the loaded rules")

	// Restoring the old content is a change again after a failure.
	require.NoError(t, os.WriteFile(path, []byte("rules:\n  - mode: discard\n"), 0o600))
	require.Eventually(t, func() bool { l, _ := rec.counts(); return l == 2 }, 5*time.Second, 10*time.Millisecond)
}

// TestWatcherFollowsConfigMapSwap reproduces how the kubelet updates a
// ConfigMap volume: the file is a symlink through `..data`, and an update
// points `..data` at a new timestamped directory.
func TestWatcherFollowsConfigMapSwap(t *testing.T) {
	dir := t.TempDir()
	v1 := filepath.Join(dir, "..2026_10_29_v1")
	v2 := filepath.Join(dir, "..2026_10_29_v2")
	require.NoError(t, os.Mkdir(v1, 0o700))
	require.NoError(t, os.Mkdir(v2, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(v1, "rules.yaml"), []byte("rules: []\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(v2, "rules.yaml"), []byte("rules:\n  - mode: discard\n"), 0o600))
	require.NoError(t, os.Symlink(filepath.Base(v1), filepath.Join(dir, "..data")))
	require.NoError(t, os.Symlink(filepath.Join("..data", "rules.yaml"), filepath.Join(dir, "rules.yaml")))

	rec := &recorder{}
	startWatch(t, filepath.Join(dir, "rules.yaml"), rec)
	require.Empty(t, rec.lastLoad())

	tmp := filepath.Join(dir, "..data_tmp")
	require.NoError(t, os.Symlink(filepath.Base(v2), tmp))
	require.NoError(t, os.Rename(tmp, filepath.Join(dir, "..data")))

	require.Eventually(t, func() bool { l, _ := rec.counts(); return l == 2 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, Discard, rec.lastLoad()[0].Mode)
}

func TestWatcherMissingFileIsAnErrorNotAFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rules.yaml")
	rec := &recorder{}
	startWatch(t, path, rec)
	_, errs := rec.counts()
	assert.Equal(t, 1, errs)

	require.NoError(t, os.WriteFile(path, []byte("rules: []\n"), 0o600))
	require.Eventually(t, func() bool { l, _ := rec.counts(); return l == 1 }, 5*time.Second, 10*time.Millisecond)
}
