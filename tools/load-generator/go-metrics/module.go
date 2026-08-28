package exporter

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/go-prometheus-exporter", New())

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":5656", nil)
		if err != nil {
			log.Fatalf("[PROM-EXPORTER] Failed to start HTTP server: %v", err)
		}
	}()
}

type (
	RootModule     struct{}
	ModuleInstance struct{}
)

var (
	_ modules.Instance = &ModuleInstance{}
	_ modules.Module   = &RootModule{}
)

func New() *RootModule {
	return &RootModule{}
}

func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &ModuleInstance{}
}

func (mi *ModuleInstance) Exports() modules.Exports {
	return modules.Exports{}
}
