package cdt

import (
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/vdumper"
	profio "github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/pkg/errors"
)

// FleetOptions parameterizes one VU's fleet of virtual dumpers. Every knob of
// load-testing-plan.md §4 is here so a run spec can freeze the full workload;
// the scenario script fills them from env. Zero shares and rates are honored
// as written — only an all-zero workload falls back to
// vdumper.DefaultWorkload(), matching vdumper.Config semantics.
type FleetOptions struct {
	// Addr is the collector agent endpoint, host:port. Required.
	Addr string `js:"addr"`
	// Pods is the fleet size: one virtual dumper (one TCP connection) each
	// (default 1). T2 runs use 1 pod per VU; T3 runs pack ~100 idle pods per
	// VU so thousands of connections do not need thousands of VUs.
	Pods int `js:"pods"`

	Namespace string `js:"namespace"`
	Service   string `js:"service"`
	// PodPrefix names pods "<podPrefix>-<vu>-<idx>", unique across VUs
	// (default: the service name).
	PodPrefix string `js:"podPrefix"`
	// Seed drives workload reproducibility; each pod derives its own stream
	// from (seed, VU id, pod index).
	Seed int64 `js:"seed"`
	// StartSpread staggers pod connects uniformly over this window so a big
	// fleet does not storm the listener (default "2s").
	StartSpread string `js:"startSpread"`

	// ThreadsPerPod: producer goroutines per pod; 0 keeps pods idle
	// (keep-alive only, the T3 shape).
	ThreadsPerPod int     `js:"threadsPerPod"`
	CallsPerSec   float64 `js:"callsPerSec"`
	DictInitial   int     `js:"dictInitial"`

	// Lifecycle timing (virtual-dumper.md §1.1): RestartInterval is the sleep
	// between incarnations ("" keeps the agent's 10 s); ChurnInterval > 0
	// turns on churn mode — every healthy incarnation disconnects abruptly
	// after roughly this long, the T5 reconnect-storm shape ("" or "0s" off).
	RestartInterval string `js:"restartInterval"`
	ChurnInterval   string `js:"churnInterval"`

	// Workload shape (load-testing-plan.md §4). DurationThresholds and
	// DurationShares use the comma-separated form of
	// vdumper.ParseDurationSpec; empty strings take the default tiers.
	DurationThresholds string  `js:"durationThresholds"`
	DurationShares     string  `js:"durationShares"`
	StackDepth         int     `js:"stackDepth"`
	SqlShare           float64 `js:"sqlShare"`
	SqlBytes           int     `js:"sqlBytes"`
	SqlDedup           float64 `js:"sqlDedup"`
	XmlShare           float64 `js:"xmlShare"`
	XmlBytes           int     `js:"xmlBytes"`
	SuspendRate        float64 `js:"suspendRate"`
	ErrorShare         float64 `js:"errorShare"`
	RequestIdShare     float64 `js:"requestIdShare"`
	CpuFraction        float64 `js:"cpuFraction"`
	WaitFraction       float64 `js:"waitFraction"`
	MemoryBytes        int     `js:"memoryBytes"`
	DictGrowthPerMin   float64 `js:"dictGrowthPerMin"`
}

func (o FleetOptions) withDefaults() FleetOptions {
	if o.Pods == 0 {
		o.Pods = 1
	}
	if o.Namespace == "" {
		o.Namespace = "load"
	}
	if o.Service == "" {
		o.Service = "load-svc"
	}
	if o.PodPrefix == "" {
		o.PodPrefix = o.Service
	}
	if o.Seed == 0 {
		o.Seed = 1
	}
	if o.StartSpread == "" {
		o.StartSpread = "2s"
	}
	return o
}

func (o FleetOptions) validate() (time.Duration, error) {
	if o.Addr == "" {
		return 0, errors.New("runFleet: addr is required (collector host:port)")
	}
	if o.Pods < 1 {
		return 0, errors.Errorf("runFleet: pods must be >= 1, got %d", o.Pods)
	}
	spread, err := time.ParseDuration(o.StartSpread)
	if err != nil {
		return 0, errors.Wrapf(err, "runFleet: bad startSpread %q", o.StartSpread)
	}
	if _, err := o.lifecycle(); err != nil {
		return 0, err
	}
	return spread, nil
}

// lifecycle parses the incarnation-timing knobs; empty strings keep the
// vdumper defaults (restart 10 s, churn off).
func (o FleetOptions) lifecycle() (struct{ restart, churn time.Duration }, error) {
	var out struct{ restart, churn time.Duration }
	var err error
	if o.RestartInterval != "" {
		if out.restart, err = time.ParseDuration(o.RestartInterval); err != nil {
			return out, errors.Wrapf(err, "runFleet: bad restartInterval %q", o.RestartInterval)
		}
	}
	if o.ChurnInterval != "" {
		if out.churn, err = time.ParseDuration(o.ChurnInterval); err != nil {
			return out, errors.Wrapf(err, "runFleet: bad churnInterval %q", o.ChurnInterval)
		}
	}
	return out, nil
}

// workload maps the flat option fields onto vdumper.Workload.
func (o FleetOptions) workload() (vdumper.Workload, error) {
	spec := vdumper.DefaultWorkload().Duration
	if o.DurationThresholds != "" || o.DurationShares != "" {
		var err error
		spec, err = vdumper.ParseDurationSpec(o.DurationThresholds, o.DurationShares)
		if err != nil {
			return vdumper.Workload{}, err
		}
	}
	return vdumper.Workload{
		Duration:               spec,
		StackDepthMean:         o.StackDepth,
		RequestIdShare:         o.RequestIdShare,
		Sql:                    vdumper.BigParamSpec{Share: o.SqlShare, MeanBytes: o.SqlBytes, DedupHitRate: o.SqlDedup},
		Xml:                    vdumper.BigParamSpec{Share: o.XmlShare, MeanBytes: o.XmlBytes},
		SuspendPerSec:          o.SuspendRate,
		ErrorShare:             o.ErrorShare,
		CpuFraction:            o.CpuFraction,
		WaitFraction:           o.WaitFraction,
		MemoryMeanBytes:        o.MemoryBytes,
		DictionaryGrowthPerMin: o.DictGrowthPerMin,
	}, nil
}

// podConfig builds the vdumper config of one fleet member. Pod names and
// seeds stay unique across VUs so the collector sees distinct pods and the
// workload streams do not correlate.
func (o FleetOptions) podConfig(workload vdumper.Workload, vuID uint64, idx int, stats vdumper.StatsListener) vdumper.Config {
	timing, _ := o.lifecycle() // validated in validate()
	return vdumper.Config{
		Namespace: o.Namespace,
		Service:   o.Service,
		PodName:   fmt.Sprintf("%s-%d-%d", o.PodPrefix, vuID, idx),
		Connection: emulator.ConnectionOpts{
			ProtocolAddress: o.Addr,
			Timeout: profio.TcpTimeout{
				ConnectTimeout: 10 * time.Second,
				SessionTimeout: 24 * time.Hour,
				ReadTimeout:    30 * time.Second,
				WriteTimeout:   5 * time.Second,
			},
		},
		DictionaryInitial:    o.DictInitial,
		ThreadsPerPod:        o.ThreadsPerPod,
		CallsPerSecPerThread: o.CallsPerSec,
		RestartInterval:      timing.restart,
		ChurnInterval:        timing.churn,
		Seed:                 o.Seed + int64(vuID)*1_000_000 + int64(idx)*1_000,
		Workload:             workload,
		Stats:                stats,
	}
}
