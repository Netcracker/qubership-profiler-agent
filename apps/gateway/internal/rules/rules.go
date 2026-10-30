// Package rules decides, per upload or per agent connection, whether the
// gateway forwards traffic or discards it. It has no Collector imports.
//
// A rule matches on the source's namespace, service, pod, and, for dumps, the
// dump type. Rules come from two places: a rules file, which is hot-reloaded,
// and the extension's inline config. File rules are evaluated first, the
// first match wins, and no match falls back to the extension's default mode.
package rules

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"sync/atomic"

	"go.yaml.in/yaml/v3"
)

// Mode is what the gateway does with matched traffic.
type Mode string

const (
	// Passthrough forwards traffic to the upstream unchanged.
	Passthrough Mode = "passthrough"
	// Discard accepts traffic, answers success, and drops it.
	Discard Mode = "discard"
)

// Valid reports whether m is a known mode.
func (m Mode) Valid() bool { return m == Passthrough || m == Discard }

// Key is a match key.
type Key string

const (
	KeyNamespace Key = "namespace"
	KeyService   Key = "service"
	KeyPod       Key = "pod"
	KeyDumpType  Key = "dump_type"
)

// Dump types, derived from the uploaded file name.
const (
	DumpTypeHeap   = "hprof"
	DumpTypeThread = "td"
	DumpTypeGC     = "gc"
	DumpTypeOther  = "other"
)

// DumpTypes lists every value the dump_type key accepts.
var DumpTypes = []string{DumpTypeHeap, DumpTypeThread, DumpTypeGC, DumpTypeOther}

// Surface is one kind of traffic and the match keys it can evaluate.
type Surface struct {
	Name string
	Keys []Key
}

var (
	// Dumps is the dump-upload surface. The upload URL carries the namespace
	// and pod but no service.
	Dumps = Surface{Name: "profiler_dumps", Keys: []Key{KeyNamespace, KeyPod, KeyDumpType}}
	// TCP is the profiler agent connection surface. The handshake carries
	// namespace, service, and pod.
	TCP = Surface{Name: "profiler_tcp", Keys: []Key{KeyNamespace, KeyService, KeyPod}}
)

// Match selects traffic by source. An empty field matches anything.
// Namespace, Service, and Pod are globs in path.Match syntax.
type Match struct {
	Namespace string   `mapstructure:"namespace" yaml:"namespace"`
	Service   string   `mapstructure:"service" yaml:"service"`
	Pod       string   `mapstructure:"pod" yaml:"pod"`
	DumpType  []string `mapstructure:"dump_type" yaml:"dump_type"`
}

// Rule maps a match to a mode.
type Rule struct {
	Match Match `mapstructure:"match" yaml:"match"`
	Mode  Mode  `mapstructure:"mode" yaml:"mode"`
}

// Attrs describes one upload or connection. Unknown fields are empty.
type Attrs struct {
	Namespace string
	Service   string
	Pod       string
	DumpType  string
}

// keys returns the match keys the rule sets.
func (m Match) keys() []Key {
	var keys []Key
	if m.Namespace != "" {
		keys = append(keys, KeyNamespace)
	}
	if m.Service != "" {
		keys = append(keys, KeyService)
	}
	if m.Pod != "" {
		keys = append(keys, KeyPod)
	}
	if len(m.DumpType) > 0 {
		keys = append(keys, KeyDumpType)
	}
	return keys
}

func (m Match) matches(a Attrs) bool {
	return glob(m.Namespace, a.Namespace) &&
		glob(m.Service, a.Service) &&
		glob(m.Pod, a.Pod) &&
		(len(m.DumpType) == 0 || slices.Contains(m.DumpType, a.DumpType))
}

func glob(pattern, value string) bool {
	if pattern == "" {
		return true
	}
	ok, _ := path.Match(pattern, value) // patterns are validated on load
	return ok
}

// Set is an ordered list of rules.
type Set []Rule

// Eval returns the mode of the first matching rule.
func (s Set) Eval(a Attrs) (Mode, bool) {
	for _, r := range s {
		if r.Match.matches(a) {
			return r.Mode, true
		}
	}
	return "", false
}

// Validate checks the rules against a surface. hasUpstream is false when the
// extension has no upstream, so a passthrough rule could not be honored.
func (s Set) Validate(surface Surface, hasUpstream bool) error {
	var errs []error
	for i, r := range s {
		if err := r.validate(surface, hasUpstream); err != nil {
			errs = append(errs, fmt.Errorf("rule %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (r Rule) validate(surface Surface, hasUpstream bool) error {
	if !r.Mode.Valid() {
		return fmt.Errorf("mode is %q; use %s or %s", r.Mode, Passthrough, Discard)
	}
	if r.Mode == Passthrough && !hasUpstream {
		return errors.New("mode is passthrough, but the extension has no upstream")
	}
	for _, k := range r.Match.keys() {
		if !slices.Contains(surface.Keys, k) {
			return fmt.Errorf("match key %q is not supported by %s; supported keys: %v", k, surface.Name, surface.Keys)
		}
	}
	for _, p := range []string{r.Match.Namespace, r.Match.Service, r.Match.Pod} {
		if _, err := path.Match(p, ""); err != nil {
			return fmt.Errorf("invalid glob %q: %w", p, err)
		}
	}
	for _, t := range r.Match.DumpType {
		if !slices.Contains(DumpTypes, t) {
			return fmt.Errorf("dump_type %q is unknown; use one of %v", t, DumpTypes)
		}
	}
	return nil
}

// usesOnly reports whether every key the rule sets is one the surface knows.
func (r Rule) usesOnly(surface Surface) bool {
	for _, k := range r.Match.keys() {
		if !slices.Contains(surface.Keys, k) {
			return false
		}
	}
	return true
}

type fileDoc struct {
	Rules []Rule `yaml:"rules"`
}

// ParseFile parses a rules file for one surface. Both extensions may share a
// file, so a rule that uses a key the surface cannot evaluate is skipped and
// counted in skipped. A key no surface knows is an error.
func ParseFile(data []byte, surface Surface, hasUpstream bool) (set Set, skipped int, err error) {
	var doc fileDoc
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	// An empty file decodes as io.EOF and means no rules.
	if err := dec.Decode(&doc); err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, fmt.Errorf("parse rules file: %w", err)
	}
	for _, r := range doc.Rules {
		if !r.usesOnly(surface) {
			skipped++
			continue
		}
		set = append(set, r)
	}
	if err := set.Validate(surface, hasUpstream); err != nil {
		return nil, 0, err
	}
	return set, skipped, nil
}

// Policy evaluates file rules, then inline rules, then the default mode. It
// is safe for concurrent use; the file rules are swapped atomically on
// reload.
type Policy struct {
	Default Mode
	Inline  Set
	file    atomic.Pointer[Set]
}

// SetFileRules replaces the file rules.
func (p *Policy) SetFileRules(s Set) { p.file.Store(&s) }

// Eval returns the effective mode for a source.
func (p *Policy) Eval(a Attrs) Mode {
	if f := p.file.Load(); f != nil {
		if m, ok := f.Eval(a); ok {
			return m
		}
	}
	if m, ok := p.Inline.Eval(a); ok {
		return m
	}
	return p.Default
}
