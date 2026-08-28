package main

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Spec is the run specification of doc/run-orchestration.md: everything a
// ceiling run needs, frozen into the artifact directory so runs stay
// comparable.
type Spec struct {
	Run struct {
		Name   string `yaml:"name"`
		TestID string `yaml:"testid"`
	} `yaml:"run"`
	Outputs string `yaml:"outputs"`

	Endpoints struct {
		K6        string `yaml:"k6"`
		VM        string `yaml:"vm"`
		Collector string `yaml:"collector"`
		// Toxiproxy is required only when a fault uses action: toxics.
		Toxiproxy string `yaml:"toxiproxy"`
	} `yaml:"endpoints"`

	Images     map[string]string `yaml:"images"`
	HelmValues string            `yaml:"helmValues"`
	Workload   map[string]string `yaml:"workload"`

	Ramp struct {
		Levels  []int `yaml:"levels"`
		Confirm struct {
			Timeout          duration `yaml:"timeout"`
			ConnectionsPerVU int      `yaml:"connectionsPerVU"`
			// ConnectionsQuery overrides the default active-connections
			// query when connectionsPerVU > 0.
			ConnectionsQuery string `yaml:"connectionsQuery"`
			// Ingest is the actual-vs-declared load check: the measured
			// ingest rate over the first plateau window of every hold must
			// stay within Tolerance of level × BytesPerVU, or the run is
			// invalid (doc/run-orchestration.md, "Workload wiring"). Zero
			// BytesPerVU disables the check (T3 idle fleets).
			Ingest struct {
				Query      string  `yaml:"query"`
				BytesPerVU float64 `yaml:"bytesPerVU"`
				Tolerance  float64 `yaml:"tolerance"`
			} `yaml:"ingest"`
		} `yaml:"confirm"`
		Hold struct {
			Min     duration `yaml:"min"`
			Max     duration `yaml:"max"`
			Sample  duration `yaml:"sample"`
			Plateau struct {
				Window         duration          `yaml:"window"`
				SlopeTolerance float64           `yaml:"slopeTolerance"`
				Series         map[string]string `yaml:"series"`
			} `yaml:"plateau"`
		} `yaml:"hold"`
	} `yaml:"ramp"`

	Detectors []Detector        `yaml:"detectors"`
	Context   map[string]string `yaml:"context"`
	Faults    []FaultSpec       `yaml:"faults"`

	Guard struct {
		GeneratorCPU struct {
			Query      string  `yaml:"query"`
			LimitCores float64 `yaml:"limitCores"`
			MaxShare   float64 `yaml:"maxShare"`
		} `yaml:"generator-cpu"`
	} `yaml:"guard"`

	Pprof struct {
		Points   []float64 `yaml:"points"`
		Seconds  int       `yaml:"seconds"`
		Profiles []string  `yaml:"profiles"`
	} `yaml:"pprof"`
}

// Detector is one saturation signal; kinds are defined in
// doc/run-orchestration.md.
type Detector struct {
	Name  string `yaml:"name"`
	Kind  string `yaml:"kind"`
	Query string `yaml:"query"`
	// Share is the sticky-share trigger fraction.
	Share float64 `yaml:"share"`
	// MinGrowth is the monotonic-growth relative trigger.
	MinGrowth float64 `yaml:"minGrowth"`
	// MinValue is an absolute floor for monotonic-growth: the detector stays
	// silent while the last sample is below it. A gauge that oscillates down
	// to zero (pending-parquet bytes between upload cycles) otherwise reads
	// every rising sawtooth edge as growth from zero.
	MinValue float64 `yaml:"minValue"`
	// Ratio is the baseline-ratio trigger multiple.
	Ratio float64 `yaml:"ratio"`
	// Grace excludes the first samples of every hold from this detector: a
	// cold start fills empty stores, and growth-shaped detectors would read
	// that fill as saturation (doc/run-orchestration.md).
	Grace duration `yaml:"grace"`
}

// FaultSpec is one scheduled injection of the fault layer
// (doc/run-orchestration.md, "Fault injection").
type FaultSpec struct {
	Name   string      `yaml:"name"`
	At     duration    `yaml:"at"`
	Action string      `yaml:"action"` // pod-delete | scale | toxics
	Target FaultTarget `yaml:"target"`
	// To is the scale action's replica target; a pointer so 0 is expressible.
	To     *int32      `yaml:"to"`
	Toxics []ToxicSpec `yaml:"toxics"`
	// Duration is the fault window of a stateful action (scale, toxics);
	// mutually exclusive with Repeat.
	Duration duration     `yaml:"duration"`
	Repeat   *FaultRepeat `yaml:"repeat"`
	// Expects names the failure signals this injection makes legitimate
	// inside its window; the checker scopes allowances from it and the
	// runner mutes same-named detectors.
	Expects []string `yaml:"expects"`
	// RestartBudget is how many §8.8 restart-or-replacement units one
	// injection legitimately produces (default 1). A grace-0 kill of a
	// collector measures at 2: the replacement pod plus one container
	// restart when its first start collides with the dying process's
	// collector.lock on the PV (T5.2 finding). Excess still latches.
	RestartBudget int `yaml:"restartBudget"`
	// Settle extends the expected-effects window past the fault / its revert.
	Settle duration `yaml:"settle"`
}

// FaultTarget names what an action acts on; the required fields depend on
// the action.
type FaultTarget struct {
	Namespace string `yaml:"namespace"`
	Pod       string `yaml:"pod"`   // pod-delete
	Kind      string `yaml:"kind"`  // scale: statefulset | deployment
	Name      string `yaml:"name"`  // scale
	Proxy     string `yaml:"proxy"` // toxics
}

// ToxicSpec is one toxiproxy toxic; attributes pass through to the REST API.
type ToxicSpec struct {
	Type       string         `yaml:"type"`
	Attributes map[string]any `yaml:"attributes"`
}

// FaultRepeat turns an instant action into a crashloop: the next injection
// waits for the target's observed READY plus Every; missing ReadyTimeout is
// a failed recovery and turns the run invalid.
type FaultRepeat struct {
	Every        duration `yaml:"every"`
	Count        int      `yaml:"count"`
	ReadyTimeout duration `yaml:"readyTimeout"`
}

// expectsVocabulary is the closed set of expected-failure signals a fault
// may declare; checker.md defines how each maps to a §8 allowance, and the
// runner mutes detectors whose name matches one of them.
var expectsVocabulary = map[string]bool{
	"restarts": true, "scrape-gap": true, "refused-bytes": true,
	"ingest-paused": true, "freshness": true, "markers": true,
	"compaction-lag": true, "small-file-share": true, "hot-window-lag": true,
	"hot-store-growth": true, "ack-errors": true, "pending-parquet-growth": true,
}

var faultNameRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

func (f *FaultSpec) validate(hasToxiproxy bool) error {
	if !faultNameRe.MatchString(f.Name) {
		return fmt.Errorf("fault %q: the name must be lowercase alphanumerics and dashes — it becomes part of toxic names, URL paths, and log keys", f.Name)
	}
	if f.At <= 0 {
		return fmt.Errorf("fault %q: at is required (offset from the hold start)", f.Name)
	}
	if f.Repeat != nil && f.Duration != 0 {
		return fmt.Errorf("fault %q: repeat and duration exclude each other", f.Name)
	}
	for _, e := range f.Expects {
		if !expectsVocabulary[e] {
			return fmt.Errorf("fault %q: unknown expects entry %q", f.Name, e)
		}
	}
	if f.Settle == 0 {
		f.Settle = duration(5 * time.Minute)
	}
	if f.RestartBudget == 0 {
		f.RestartBudget = 1
	}
	if f.RestartBudget < 1 {
		return fmt.Errorf("fault %q: restartBudget must be >= 1", f.Name)
	}
	switch f.Action {
	case "pod-delete":
		if f.Target.Namespace == "" || f.Target.Pod == "" {
			return fmt.Errorf("fault %q: pod-delete needs target.namespace and target.pod", f.Name)
		}
		if f.Duration != 0 || f.To != nil || len(f.Toxics) > 0 {
			return fmt.Errorf("fault %q: pod-delete is instant — no duration, to, or toxics", f.Name)
		}
		if f.Repeat != nil {
			if f.Repeat.Every <= 0 || f.Repeat.Count <= 0 {
				return fmt.Errorf("fault %q: repeat needs every > 0 and count > 0", f.Name)
			}
			if f.Repeat.ReadyTimeout == 0 {
				f.Repeat.ReadyTimeout = duration(5 * time.Minute)
			}
		}
	case "scale":
		if f.Target.Namespace == "" || f.Target.Name == "" ||
			(f.Target.Kind != "statefulset" && f.Target.Kind != "deployment") {
			return fmt.Errorf("fault %q: scale needs target.namespace, target.name, and kind statefulset|deployment", f.Name)
		}
		if f.To == nil {
			return fmt.Errorf("fault %q: scale needs to (the replica target)", f.Name)
		}
		if f.Duration <= 0 {
			return fmt.Errorf("fault %q: a stateful action needs duration", f.Name)
		}
		if f.Repeat != nil {
			return fmt.Errorf("fault %q: repeat applies to instant actions only", f.Name)
		}
	case "toxics":
		if f.Target.Proxy == "" {
			return fmt.Errorf("fault %q: toxics needs target.proxy", f.Name)
		}
		if len(f.Toxics) == 0 {
			return fmt.Errorf("fault %q: toxics needs at least one toxic", f.Name)
		}
		for _, tx := range f.Toxics {
			if tx.Type == "" {
				return fmt.Errorf("fault %q: every toxic needs a type", f.Name)
			}
		}
		if f.Duration <= 0 {
			return fmt.Errorf("fault %q: a stateful action needs duration", f.Name)
		}
		if f.Repeat != nil {
			return fmt.Errorf("fault %q: repeat applies to instant actions only", f.Name)
		}
		if !hasToxiproxy {
			return fmt.Errorf("fault %q: action toxics needs endpoints.toxiproxy", f.Name)
		}
	default:
		return fmt.Errorf("fault %q: unknown action %q", f.Name, f.Action)
	}
	return nil
}

// duration wraps time.Duration for YAML ("3m", "15s").
type duration time.Duration

func (d *duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = duration(v)
	return nil
}

func (d duration) std() time.Duration { return time.Duration(d) }

var detectorKinds = map[string]bool{
	"sticky-share": true, "monotonic-growth": true, "nonzero": true, "baseline-ratio": true,
}

// LoadSpec reads and validates a run spec.
func LoadSpec(path string) (*Spec, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Spec
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &s, s.validate()
}

func (s *Spec) validate() error {
	switch {
	case s.Run.Name == "":
		return fmt.Errorf("run.name is required")
	case s.Run.TestID == "":
		return fmt.Errorf("run.testid is required — the unique run label keeps VM series of different runs apart")
	case s.Endpoints.K6 == "" || s.Endpoints.VM == "" || s.Endpoints.Collector == "":
		return fmt.Errorf("endpoints.k6, endpoints.vm, and endpoints.collector are all required")
	case len(s.Ramp.Levels) == 0:
		return fmt.Errorf("ramp.levels must list at least one VU step")
	case len(s.Ramp.Hold.Plateau.Series) == 0:
		return fmt.Errorf("ramp.hold.plateau.series must name at least one series (what would the hold wait for?)")
	}
	prev := 0
	for _, l := range s.Ramp.Levels {
		if l <= prev {
			return fmt.Errorf("ramp.levels must increase strictly, got %v", s.Ramp.Levels)
		}
		prev = l
	}
	for _, d := range s.Detectors {
		if !detectorKinds[d.Kind] {
			return fmt.Errorf("detector %q: unknown kind %q", d.Name, d.Kind)
		}
		if d.Query == "" {
			return fmt.Errorf("detector %q: query is required", d.Name)
		}
	}
	if s.Outputs == "" {
		s.Outputs = "runs"
	}
	if s.Ramp.Confirm.Timeout == 0 {
		s.Ramp.Confirm.Timeout = duration(3 * time.Minute)
	}
	if s.Ramp.Hold.Min == 0 {
		s.Ramp.Hold.Min = duration(3 * time.Minute)
	}
	if s.Ramp.Hold.Max == 0 {
		s.Ramp.Hold.Max = duration(15 * time.Minute)
	}
	if s.Ramp.Hold.Sample == 0 {
		s.Ramp.Hold.Sample = duration(15 * time.Second)
	}
	if s.Ramp.Hold.Plateau.Window == 0 {
		s.Ramp.Hold.Plateau.Window = duration(2 * time.Minute)
	}
	if s.Ramp.Hold.Plateau.SlopeTolerance == 0 {
		s.Ramp.Hold.Plateau.SlopeTolerance = 0.05
	}
	if len(s.Faults) > 0 {
		if len(s.Ramp.Levels) != 1 {
			return fmt.Errorf("faults need a single-level spec: the schedule counts from the one hold's start")
		}
		seen := map[string]bool{}
		for i := range s.Faults {
			f := &s.Faults[i]
			if seen[f.Name] {
				return fmt.Errorf("fault %q: names must be unique across the spec", f.Name)
			}
			seen[f.Name] = true
			if err := f.validate(s.Endpoints.Toxiproxy != ""); err != nil {
				return err
			}
		}
	}
	if ing := &s.Ramp.Confirm.Ingest; ing.BytesPerVU > 0 {
		if ing.Tolerance == 0 {
			ing.Tolerance = 0.25
		}
		if ing.Query == "" {
			ing.Query = s.Ramp.Hold.Plateau.Series["ingest-bytes"]
			if ing.Query == "" {
				return fmt.Errorf("confirm.ingest needs a query: set confirm.ingest.query or name a plateau series ingest-bytes")
			}
		}
	}
	if len(s.Pprof.Points) == 0 {
		s.Pprof.Points = []float64{0.7, 1.0}
	}
	if s.Pprof.Seconds == 0 {
		s.Pprof.Seconds = 30
	}
	if len(s.Pprof.Profiles) == 0 {
		s.Pprof.Profiles = []string{"profile", "heap", "goroutine"}
	}
	return nil
}
