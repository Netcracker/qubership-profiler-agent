package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// artifacts owns the run's output directory (doc/run-orchestration.md):
// spec + values snapshot, steps.jsonl, series/, pprof/, result.json.
type artifacts struct {
	dir   string
	steps *os.File
}

func newArtifacts(spec *Spec, specPath string, startedAt time.Time) (*artifacts, error) {
	dir := filepath.Join(spec.Outputs,
		fmt.Sprintf("%s-%s", startedAt.UTC().Format("20060102T150405Z"), spec.Run.Name))
	for _, sub := range []string{"", "series", "pprof"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	// Freeze the spec and the Helm values snapshot before the first scale.
	if err := copyFile(specPath, filepath.Join(dir, "spec.yaml")); err != nil {
		return nil, err
	}
	if spec.HelmValues != "" {
		if err := copyFile(spec.HelmValues, filepath.Join(dir, "values-snapshot.yaml")); err != nil {
			return nil, fmt.Errorf("helmValues: %w", err)
		}
	}
	steps, err := os.Create(filepath.Join(dir, "steps.jsonl"))
	if err != nil {
		return nil, err
	}
	return &artifacts{dir: dir, steps: steps}, nil
}

func (a *artifacts) pprofDir() string { return filepath.Join(a.dir, "pprof") }

func (a *artifacts) appendStep(rec stepRecord) error {
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := a.steps.Write(append(line, '\n')); err != nil {
		return err
	}
	return a.steps.Sync()
}

// exportSeries archives every named query over the whole run window.
func (a *artifacts) exportSeries(ctx context.Context, vm *vmClient, queries map[string]string,
	from, to time.Time, step time.Duration) error {

	for name, q := range queries {
		raw, err := vm.Range(ctx, q, from, to, step)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if err := os.WriteFile(filepath.Join(a.dir, "series", name+".json"), raw, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (a *artifacts) writeResult(res runResult) error {
	raw, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(a.dir, "result.json"), raw, 0o644); err != nil {
		return err
	}
	return a.steps.Close()
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
