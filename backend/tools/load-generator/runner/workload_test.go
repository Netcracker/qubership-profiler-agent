package main

import (
	"strings"
	"testing"
)

func TestVerifyWorkloadMatch(t *testing.T) {
	spec := map[string]string{"CALLS_PER_SEC": "3", "THREADS_PER_POD": "8"}
	fp := map[string][]string{"CALLS_PER_SEC": {"3"}, "THREADS_PER_POD": {"8"}}
	incomplete, err := verifyWorkload(spec, fp)
	if err != nil || incomplete {
		t.Fatalf("expected a clean match, got incomplete=%v err=%v", incomplete, err)
	}
}

func TestVerifyWorkloadValueMismatchIsHard(t *testing.T) {
	spec := map[string]string{"CALLS_PER_SEC": "3"}
	fp := map[string][]string{"CALLS_PER_SEC": {"5"}}
	incomplete, err := verifyWorkload(spec, fp)
	if err == nil {
		t.Fatal("expected a mismatch error")
	}
	if incomplete {
		t.Fatal("a value mismatch must be hard, not retryable")
	}
	if !strings.Contains(err.Error(), "CALLS_PER_SEC") {
		t.Fatalf("error must name the knob: %v", err)
	}
}

func TestVerifyWorkloadStringEqualityIsExact(t *testing.T) {
	spec := map[string]string{"SQL_SHARE": "0.2"}
	fp := map[string][]string{"SQL_SHARE": {"0.20"}}
	if _, err := verifyWorkload(spec, fp); err == nil {
		t.Fatal("comparison is env-level string equality: \"0.2\" != \"0.20\"")
	}
}

func TestVerifyWorkloadMissingKnobIsRetryable(t *testing.T) {
	spec := map[string]string{"CALLS_PER_SEC": "3", "SEED": "1"}
	fp := map[string][]string{"CALLS_PER_SEC": {"3"}}
	incomplete, err := verifyWorkload(spec, fp)
	if err == nil {
		t.Fatal("expected a missing-knob error")
	}
	if !incomplete {
		t.Fatal("a missing knob is retryable while remote write lags")
	}
	if !strings.Contains(err.Error(), "SEED") {
		t.Fatalf("error must name the knob: %v", err)
	}
}

func TestVerifyWorkloadUnexpectedKnobIsHard(t *testing.T) {
	spec := map[string]string{"CALLS_PER_SEC": "3"}
	fp := map[string][]string{"CALLS_PER_SEC": {"3"}, "SEED": {"1"}}
	incomplete, err := verifyWorkload(spec, fp)
	if err == nil {
		t.Fatal("a deployment knob absent from the spec breaks the complete freeze")
	}
	if incomplete {
		t.Fatal("an unexpected knob is hard, not retryable")
	}
}

func TestVerifyWorkloadConflictingValuesAreHard(t *testing.T) {
	spec := map[string]string{"CALLS_PER_SEC": "3"}
	fp := map[string][]string{"CALLS_PER_SEC": {"3", "5"}}
	incomplete, err := verifyWorkload(spec, fp)
	if err == nil {
		t.Fatal("two values under one testid mean a reused testid")
	}
	if incomplete {
		t.Fatal("conflicting values are hard, not retryable")
	}
}

func TestVerifyWorkloadMismatchWinsOverMissing(t *testing.T) {
	// A hard mismatch must not be downgraded to retryable by a knob that is
	// also missing.
	spec := map[string]string{"CALLS_PER_SEC": "3", "SEED": "1"}
	fp := map[string][]string{"CALLS_PER_SEC": {"5"}}
	incomplete, err := verifyWorkload(spec, fp)
	if err == nil || incomplete {
		t.Fatalf("expected a hard error, got incomplete=%v err=%v", incomplete, err)
	}
}

func TestCheckIngestWithinTolerance(t *testing.T) {
	// 20 VUs × 19650 = 393000; 15% off passes at tolerance 0.25.
	if err := checkIngest(20, 19650, 0.25, 393000*1.15); err != nil {
		t.Fatalf("expected pass: %v", err)
	}
}

func TestCheckIngestTooHighFails(t *testing.T) {
	// The phase-4 wiring defect: declared 3 calls/s, actual 5 — 1.9× measured.
	err := checkIngest(20, 19650, 0.25, 761000)
	if err == nil {
		t.Fatal("a 1.9× ingest rate must fail the check")
	}
	if !strings.Contains(err.Error(), "ingest-confirm") {
		t.Fatalf("error must be attributable: %v", err)
	}
}

func TestCheckIngestTooLowFails(t *testing.T) {
	if err := checkIngest(20, 19650, 0.25, 100000); err == nil {
		t.Fatal("an under-delivering generator must fail the check too")
	}
}
