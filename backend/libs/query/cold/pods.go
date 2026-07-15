package cold

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// podsManifest is the per-(day, pod-restart) identity object the upload pass
// writes (01-write-contract.md §3.6). Fields the read path does not need are
// left out; unknown JSON members are ignored.
type podsManifest struct {
	Namespace     string `json:"namespace"`
	Service       string `json:"service"`
	Pod           string `json:"pod"`
	RestartTimeMs int64  `json:"restart_time_ms"`
	TimeMinMs     int64  `json:"time_min_ms"`
	TimeMaxMs     int64  `json:"time_max_ms"`
}

// PodsResult carries the cold /pods entries plus the §7.4 partial markers.
type PodsResult struct {
	Pods           []model.PodEntry
	PartialReasons []string
	Prefixes       int
	FailedPrefixes int
}

// Pods lists the closed pod-restarts with data in [fromMs, toMs) from the
// pods/v1 manifests (02 §2.7): one LIST per UTC day the range spans, one GET
// per manifest, no parquet file opened. A manifest deleted between the LIST
// and the GET is skipped (§5.1's 404-as-empty, same backstop as parquet).
func (s *Source) Pods(ctx context.Context, fromMs, toMs int64) (PodsResult, error) {
	days := dayWalk(fromMs, toMs)
	res := PodsResult{Prefixes: len(days)}

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, s.listConcurrency())
	for _, day := range days {
		wg.Add(1)
		go func(day time.Time) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			prefix := path.Join("pods/v1", day.Format("2006/01/02")) + "/"
			objects, err := s.Store.List(ctx, prefix)
			if err != nil {
				mu.Lock()
				res.FailedPrefixes++
				res.PartialReasons = append(res.PartialReasons, fmt.Sprintf("s3 list %s: %v", prefix, err))
				mu.Unlock()
				return
			}
			for _, obj := range objects {
				tuple, ok, err := s.readManifest(ctx, obj.Key, fromMs, toMs)
				if err != nil {
					mu.Lock()
					res.PartialReasons = append(res.PartialReasons, fmt.Sprintf("s3 get %s: %v", obj.Key, err))
					mu.Unlock()
					continue
				}
				if ok {
					mu.Lock()
					res.Pods = append(res.Pods, tuple)
					mu.Unlock()
				}
			}
		}(day)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return res, err
	}

	// A pod-restart spanning several days has one manifest per day (01 §3.6):
	// the union collapses them into one entry with the widened bounds and a
	// stable order (02 §2.7).
	res.Pods = model.UnionPods(res.Pods)
	return res, nil
}

func (s *Source) readManifest(ctx context.Context, key string, fromMs, toMs int64) (model.PodEntry, bool, error) {
	body, err := s.Store.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return model.PodEntry{}, false, nil
	}
	if err != nil {
		return model.PodEntry{}, false, err
	}
	var m podsManifest
	if err := json.Unmarshal(body, &m); err != nil {
		return model.PodEntry{}, false, errors.Wrap(err, "decode pods manifest")
	}
	if m.TimeMinMs >= toMs || m.TimeMaxMs < fromMs {
		return model.PodEntry{}, false, nil // no Call rows inside the window
	}
	return model.PodEntry{
		PodTuple: model.PodTuple{
			Namespace:     m.Namespace,
			Service:       m.Service,
			Pod:           m.Pod,
			RestartTimeMs: m.RestartTimeMs,
		},
		TimeMinMs: m.TimeMinMs,
		TimeMaxMs: m.TimeMaxMs,
	}, true, nil
}

// dayWalk lists the UTC days spanning [fromMs, toMs).
func dayWalk(fromMs, toMs int64) []time.Time {
	if toMs <= fromMs {
		return nil
	}
	first := time.UnixMilli(fromMs).UTC().Truncate(24 * time.Hour)
	last := time.UnixMilli(toMs - 1).UTC().Truncate(24 * time.Hour)
	var days []time.Time
	for d := first; !d.After(last); d = d.Add(24 * time.Hour) {
		days = append(days, d)
	}
	return days
}
