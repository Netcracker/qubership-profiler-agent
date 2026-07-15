package cold

import (
	"context"
	"encoding/json"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// dictionarySnapshot mirrors the S3 object shape (01-write-contract.md §3.6,
// 02 §2.6). The wire dictionary is one id space, so methods and params carry
// the same full word list; either array resolves any id.
type dictionarySnapshot struct {
	Version int      `json:"version"`
	Methods []string `json:"methods"`
	Params  []string `json:"params"`
}

// Dictionary fetches a closed pod-restart's dictionary snapshot: the words
// array indexed by dictionary id. The key derives from the UTC day of
// restart_time_ms (01 §3.6); ok is false when the snapshot object does not
// exist — the pod-restart never closed cleanly, or its TTL expired.
func Dictionary(ctx context.Context, store ObjectStore, tuple model.PodTuple) ([]string, bool, error) {
	key := model.DictionarySnapshotKey(tuple)
	body, err := store.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.Wrapf(err, "get %s", key)
	}
	var snap dictionarySnapshot
	if err := json.Unmarshal(body, &snap); err != nil {
		return nil, false, errors.Wrapf(err, "decode %s", key)
	}
	return snap.Methods, true, nil
}
