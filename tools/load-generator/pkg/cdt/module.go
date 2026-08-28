// Package cdt is the k6/x/cdt module: one VU drives a fleet of virtual
// dumpers (libs/emulator/vdumper) against a collector and maps the fleet's
// StatsListener events to k6 samples. The wire behavior lives entirely in
// vdumper; this package is glue — options parsing, pod fan-out, metrics.
//
// The run orchestrator (tools/load-generator/runner) scales the fleet count
// over the k6 REST API with the externally-controlled executor; see
// doc/run-orchestration.md.
package cdt

import (
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/cdt", New())
}

type (
	// RootModule is the per-test module state; instances share nothing.
	RootModule struct{}
	// ModuleInstance is the per-VU module state.
	ModuleInstance struct {
		vu      modules.VU
		metrics fleetMetrics
	}
)

var (
	_ modules.Module   = &RootModule{}
	_ modules.Instance = &ModuleInstance{}
)

// New builds the module for k6's registry.
func New() *RootModule { return &RootModule{} }

// NewModuleInstance registers the vdumper metrics and binds the module to the
// VU.
func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	m, err := registerMetrics(vu.InitEnv().Registry)
	if err != nil {
		common.Throw(vu.Runtime(), err)
	}
	return &ModuleInstance{vu: vu, metrics: m}
}

// Exports exposes the module to JS; methods surface with a lowercase first
// letter (runFleet).
func (mi *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{Default: mi}
}
