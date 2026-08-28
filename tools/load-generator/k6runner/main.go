// Command k6runner is the custom k6 binary of the load-testing harness: k6
// plus the k6/x/cdt fleet module and the go-prometheus-exporter module,
// linked with a plain `go build` instead of the xk6 CLI — the modules live in
// this repository's single Go module, so no module-path juggling is needed.
package main

import (
	"go.k6.io/k6/cmd"

	_ "github.com/Netcracker/qubership-profiler-backend/tools/load-generator/go-metrics"
	_ "github.com/Netcracker/qubership-profiler-backend/tools/load-generator/pkg/cdt"
)

func main() {
	cmd.Execute()
}
