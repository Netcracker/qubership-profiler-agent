// Command runner orchestrates one ceiling run (doc/run-orchestration.md): it
// ramps the k6 fleet over the REST API step by step, holds each step until
// the key series flatten, judges saturation, captures pprof profiles at the
// ceiling shares, and archives the artifacts that make runs comparable.
//
// Usage:
//
//	kubectl -n profiler-load port-forward svc/cdt-loader-service 6565:6565 &
//	kubectl -n monitoring port-forward svc/vmsingle-k8s 8429:8429 &
//	kubectl -n profiler-load port-forward profiler-backend-collector-0 8081:8081 &
//	go run ./tools/load-generator/runner -spec t2-bytes.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	specPath := flag.String("spec", "", "run spec YAML (required)")
	revertFaults := flag.Bool("revert-faults", false, "revert stale fault state left by dead runs (skips runs protected by a live stand lease), then exit")
	outputs := flag.String("outputs", "runs", "runs directory holding the fault registry and the stand lease (for -revert-faults)")
	flag.Parse()
	if *revertFaults {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		runRevertFaults(ctx, *outputs)
		return
	}
	if *specPath == "" {
		fmt.Fprintln(os.Stderr, "runner: -spec is required; see doc/run-orchestration.md")
		os.Exit(2)
	}

	spec, err := LoadSpec(*specPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startedAt := time.Now()
	art, err := newArtifacts(spec, *specPath, startedAt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("runner: %s (testid %s) -> %s\n", spec.Run.Name, spec.Run.TestID, art.dir)

	r := &runner{
		spec:      spec,
		k6:        newK6Client(spec.Endpoints.K6),
		vm:        newVMClient(spec.Endpoints.VM),
		art:       art,
		baselines: map[string]float64{},
	}
	res, err := r.Run(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner: aborted: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("runner: %s — verdict %s", spec.Run.Name, res.Verdict)
	if res.Verdict == "saturated" {
		fmt.Printf(", ceiling %d VUs (saturated at %d: %v)", res.CeilingLevel, res.SaturatedLevel, res.FiringDetectors)
	}
	fmt.Println()
	if res.Verdict == "invalid" {
		os.Exit(1)
	}
}
