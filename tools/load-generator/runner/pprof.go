package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// capturePprof pulls one profile from the collector's internal port
// (PROFILER_PPROF_ENABLED=true) and writes it under the run's pprof/ dir.
// point is the ceiling share the capture belongs to (0.7, 1.0).
func capturePprof(ctx context.Context, collectorBase, dir, profile string, seconds int, point float64) (string, error) {
	url := fmt.Sprintf("%s/debug/pprof/%s", collectorBase, profile)
	timeout := 30 * time.Second
	if profile == "profile" {
		url = fmt.Sprintf("%s?seconds=%d", url, seconds)
		timeout = time.Duration(seconds+30) * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		return "", fmt.Errorf("pprof %s: HTTP %d: %s", profile, resp.StatusCode, msg)
	}

	name := profile
	if profile == "profile" {
		name = "cpu"
	}
	path := filepath.Join(dir, fmt.Sprintf("%s-%.0fpct.pb.gz", name, point*100))
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return "", err
	}
	return path, nil
}
