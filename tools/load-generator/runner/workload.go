package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
)

// The scenario emits its workload fingerprint once, at test start; an instant
// query would lose it to staleness, so the lookup reaches back over a window
// generously longer than any deploy-to-run gap.
const fingerprintLookback = "24h"

// fetchFingerprint reads the k6 deployment's workload fingerprint
// (k6_workload_info samples emitted by the scenario's setup) as knob → seen
// values. Multiple values for one knob survive into the result so the caller
// can reject a reused testid.
func (r *runner) fetchFingerprint(ctx context.Context) (map[string][]string, error) {
	query := fmt.Sprintf("last_over_time(k6_workload_info{testid=%q}[%s])",
		r.spec.Run.TestID, fingerprintLookback)
	samples, err := r.vm.InstantVector(ctx, query)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, s := range samples {
		knob, value := s.Metric["knob"], s.Metric["value"]
		if knob == "" {
			continue
		}
		if !contains(out[knob], value) {
			out[knob] = append(out[knob], value)
		}
	}
	return out, nil
}

// verifyWorkload compares the spec's frozen workload block against the
// deployment's fingerprint: exact string equality, both directions
// (doc/run-orchestration.md, "Workload wiring"). The returned incomplete flag
// distinguishes "knobs not exported yet" (retryable within the confirm
// timeout: remote write lags) from a hard mismatch.
func verifyWorkload(spec map[string]string, fingerprint map[string][]string) (incomplete bool, err error) {
	var missing, conflicting, mismatched, unexpected []string
	for knob, want := range spec {
		got, ok := fingerprint[knob]
		switch {
		case !ok:
			missing = append(missing, knob)
		case len(got) > 1:
			conflicting = append(conflicting, fmt.Sprintf("%s=%v", knob, got))
		case got[0] != want:
			mismatched = append(mismatched, fmt.Sprintf("%s: spec %q, deployment %q", knob, want, got[0]))
		}
	}
	for knob := range fingerprint {
		if _, ok := spec[knob]; !ok {
			unexpected = append(unexpected, knob)
		}
	}
	sort.Strings(missing)
	sort.Strings(conflicting)
	sort.Strings(mismatched)
	sort.Strings(unexpected)

	var hard []string
	if len(conflicting) > 0 {
		hard = append(hard, "several values under one testid (reused testid?): "+strings.Join(conflicting, ", "))
	}
	if len(mismatched) > 0 {
		hard = append(hard, "value mismatch: "+strings.Join(mismatched, "; "))
	}
	if len(unexpected) > 0 {
		hard = append(hard, "deployment knobs absent from the spec (the workload block is a complete freeze): "+
			strings.Join(unexpected, ", "))
	}
	if len(hard) > 0 {
		return false, fmt.Errorf("workload fingerprint: %s", strings.Join(hard, "; "))
	}
	if len(missing) > 0 {
		return true, fmt.Errorf("workload fingerprint: spec knobs not exported by the deployment: %s",
			strings.Join(missing, ", "))
	}
	return false, nil
}

// checkIngest judges the actual-vs-declared load of one hold: the measured
// mean must stay within tolerance of level × bytesPerVU.
func checkIngest(level int, bytesPerVU, tolerance, measured float64) error {
	expected := float64(level) * bytesPerVU
	if expected <= 0 {
		return nil
	}
	deviation := math.Abs(measured-expected) / expected
	if deviation > tolerance {
		return fmt.Errorf("ingest-confirm: measured %.0f bytes/s vs declared %.0f (%d VUs × %.0f), deviation %.0f%% > %.0f%%",
			measured, expected, level, bytesPerVU, deviation*100, tolerance*100)
	}
	return nil
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}
