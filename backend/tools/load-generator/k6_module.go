package wireshark

import (
	"github.com/Netcracker/qubership-profiler-backend/tools/load-generator/pkg/cdt"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

type CdtRootModule struct{}

func init() {
	modules.Register("k6/x/cdt", new(CdtRootModule))
}

func (*CdtRootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	m, err := cdt.RegisterMetrics(vu)
	if err != nil {
		common.Throw(vu.Runtime(), err)
	}
	return &cdt.Collector{VU: vu, Metrics: m}
}
