package model

import (
	"crypto/sha256"
	"encoding/hex"
	"path"
	"strconv"
	"time"
)

// PodRestartHash renders the short one-way hash that stands in for the
// pod-restart identity tuple in sealed-file names and snapshot keys
// (01-write-contract.md §7, §3.6). The write side (libs/collector/hotstore)
// and the cold read side resolve keys through this one function, so the two
// can never disagree on an object's location.
func PodRestartHash(t PodTuple) string {
	id := t.Namespace + "/" + t.Service + "/" + t.Pod + "/" + strconv.FormatInt(t.RestartTimeMs, 10)
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:4])
}

// DictionarySnapshotKey is the S3 key of a closed pod-restart's dictionary
// snapshot (01-write-contract.md §3.6). The day component derives from
// restart_time_ms — not from the close or query day — because only the
// restart time is crash-stable and derivable by any reader that already holds
// the pod-restart tuple (stage1-progress.md, 2026-07-03).
func DictionarySnapshotKey(t PodTuple) string {
	day := time.UnixMilli(t.RestartTimeMs).UTC().Format("2006/01/02")
	return path.Join("dictionaries/v1", day, PodRestartHash(t)+".json")
}

// SuspendSnapshotKey is the S3 key of a closed pod-restart's stop-the-world
// timeline snapshot, uploaded alongside the dictionary (01-write-contract.md
// §3.6) and read by the cold /tree path for the R7 per-node suspension. The
// day derives from restart_time_ms for the same reasons as the dictionary
// key.
func SuspendSnapshotKey(t PodTuple) string {
	day := time.UnixMilli(t.RestartTimeMs).UTC().Format("2006/01/02")
	return path.Join("suspend/v1", day, PodRestartHash(t)+".json")
}
