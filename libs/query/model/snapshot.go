package model

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// PodRestartHash renders the short one-way hash that stands in for the
// pod-restart identity tuple in sealed-file names and the pods/v1 manifest
// keys (01-write-contract.md §7, §3.6). The write side
// (libs/collector/hotstore) and the cold read side resolve keys through this
// one function, so the two can never disagree on an object's location.
//
// 12 bytes (24 hex chars) of SHA-256 (№22): the hash keys parquet files and
// the pods manifests, so a collision would silently merge two pod-restarts'
// objects. The previous 4 bytes gave 50% collision odds around 77k
// pod-restarts — a few weeks of a 500-pod cluster's restart churn inside one
// retention window; 96 bits puts the birthday bound beyond any realistic
// object count.
func PodRestartHash(t PodTuple) string {
	id := t.Namespace + "/" + t.Service + "/" + t.Pod + "/" + strconv.FormatInt(t.RestartTimeMs, 10)
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:12])
}
