// Fault-state recovery: restore whatever a dead run left behind
// (doc/run-orchestration.md, "Durable revert"). Runs standalone as
// `runner -revert-faults` and automatically in every preflight.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// revertStaleFaults sweeps runs/.active-faults and reverts every entry whose
// run is not protected by a live lease. It returns the number of reverted
// entries; any failure aborts — a stand with un-reverted faults must not
// take a run.
func revertStaleFaults(ctx context.Context, outputs string) (int, error) {
	lease, err := readLease(outputs)
	if err != nil {
		return 0, err
	}
	liveTestID := ""
	if lease != nil && lease.live(time.Now()) {
		liveTestID = lease.TestID
	}

	dir := activeFaultsDir(outputs)
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return 0, err
	}
	reverted := 0
	for _, path := range files {
		testid := strings.TrimSuffix(filepath.Base(path), ".json")
		if testid == liveTestID {
			continue // owned by the running run; reverting it would corrupt that run
		}
		reg, err := openFaultRegistry(outputs, testid)
		if err != nil {
			return reverted, err
		}
		for _, e := range reg.entries() {
			if err := revertEntry(ctx, e); err != nil {
				return reverted, fmt.Errorf("revert %s (run %s): %w", e.FaultID, testid, err)
			}
			if err := reg.remove(e.FaultID); err != nil {
				return reverted, err
			}
			fmt.Printf("runner: reverted stale fault %s of run %s\n", e.FaultID, testid)
			reverted++
		}
	}
	return reverted, nil
}

func revertEntry(ctx context.Context, e registryEntry) error {
	switch e.Action {
	case "scale":
		kube, err := newKubeClient()
		if err != nil {
			return err
		}
		return kube.SetScale(ctx, e.Target.Namespace, e.Target.Kind, e.Target.Name, e.PriorReplicas)
	case "toxics":
		if e.ToxiproxyURL == "" {
			return fmt.Errorf("registry entry carries no toxiproxy URL")
		}
		toxi := newToxiClient(e.ToxiproxyURL)
		for _, name := range e.ToxicNames {
			if err := toxi.DeleteToxic(ctx, e.Target.Proxy, name); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown registry action %q", e.Action)
	}
}

// runRevertFaults is the standalone CLI mode.
func runRevertFaults(ctx context.Context, outputs string) {
	n, err := revertStaleFaults(ctx, outputs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner: revert-faults: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("runner: revert-faults done, %d entr%s reverted\n", n, plural(n))
}

func plural(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}
