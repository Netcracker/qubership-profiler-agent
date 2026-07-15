package cold

import (
	"context"
	"encoding/json"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// suspendSnapshot mirrors the suspend/v1 S3 object the uploader writes
// alongside the dictionary snapshot (01-write-contract.md §3.6).
type suspendSnapshot struct {
	Events []struct {
		// EndMs is the pause end; the pause spans [EndMs − DurationMs, EndMs]
		// (calltree treats SuspendInterval.TimeMs as the end) (№4).
		EndMs      int64 `json:"end_ms"`
		DurationMs int64 `json:"duration_ms"`
	} `json:"events"`
}

// Suspend fetches a closed pod-restart's stop-the-world timeline for the R7
// per-node suspension (08-ui-backend-requirements.md). ok is false when the
// snapshot object does not exist — the pod-restart never closed cleanly or
// its TTL expired — and the caller degrades to zero suspension.
func Suspend(ctx context.Context, store ObjectStore, tuple model.PodTuple) ([]calltree.SuspendInterval, bool, error) {
	key := model.SuspendSnapshotKey(tuple)
	body, err := store.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.Wrapf(err, "get %s", key)
	}
	var snap suspendSnapshot
	if err := json.Unmarshal(body, &snap); err != nil {
		return nil, false, errors.Wrapf(err, "decode %s", key)
	}
	pauses := make([]calltree.SuspendInterval, 0, len(snap.Events))
	for _, e := range snap.Events {
		pauses = append(pauses, calltree.SuspendInterval{TimeMs: e.EndMs, DurationMs: e.DurationMs})
	}
	return pauses, true, nil
}
