package stats

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"slices"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// RowValues are the members every /stats row carries (02 §2.9). The
// percentiles are null for a row with no calls.
type RowValues struct {
	Count          int64  `json:"count"`
	Errors         int64  `json:"errors"`
	DurationMsSum  int64  `json:"duration_ms_sum"`
	P50Ms          *int64 `json:"p50_ms"`
	P95Ms          *int64 `json:"p95_ms"`
	P99Ms          *int64 `json:"p99_ms"`
	CPUTimeMsSum   int64  `json:"cpu_time_ms_sum"`
	WaitTimeMsSum  int64  `json:"wait_time_ms_sum"`
	QueueWaitMsSum int64  `json:"queue_wait_ms_sum"`
	SuspendMsSum   int64  `json:"suspend_ms_sum"`
}

// Row is one /stats row. Key is the bucket start in Unix ms (int64) for a
// time grouping and the field's value (string) otherwise.
type Row struct {
	Key any `json:"key"`
	RowValues
}

// Response is the body of GET /api/v1/stats (02 §2.9).
type Response struct {
	GroupBy string `json:"group_by"`
	// BucketMs is set for a time grouping only.
	BucketMs       *int64     `json:"bucket_ms,omitempty"`
	Rows           []Row      `json:"rows"`
	Other          *RowValues `json:"other"`
	Distinct       int        `json:"distinct"`
	Totals         Totals     `json:"totals"`
	Approximate    bool       `json:"approximate"`
	Partial        bool       `json:"partial"`
	PartialReasons []string   `json:"partial_reasons"`
}

// InternalGroup is one group of /internal/v1/stats: the sums and the sketch
// bins, which merge without loss, instead of percentiles (02 §3). The key is
// always text; a time group's key is the decimal bucket start.
type InternalGroup struct {
	Key            string     `json:"key"`
	Count          int64      `json:"count"`
	Errors         int64      `json:"errors"`
	DurationMsSum  int64      `json:"duration_ms_sum"`
	CPUTimeMsSum   int64      `json:"cpu_time_ms_sum"`
	WaitTimeMsSum  int64      `json:"wait_time_ms_sum"`
	QueueWaitMsSum int64      `json:"queue_wait_ms_sum"`
	SuspendMsSum   int64      `json:"suspend_ms_sum"`
	Sketch         SketchJSON `json:"sketch"`
}

// InternalResponse is the body of /internal/v1/stats: every group of the
// replica, its totals, and the PK hashes of the calls it counted.
type InternalResponse struct {
	GroupBy     string          `json:"group_by"`
	Groups      []InternalGroup `json:"groups"`
	Totals      Totals          `json:"totals"`
	Approximate bool            `json:"approximate"`
	// PKHashes is EncodeHashes of the PK hashes.
	PKHashes string `json:"pk_hashes"`
}

// InternalGroupOf renders one group for the wire.
func InternalGroupOf(g Group) InternalGroup {
	return InternalGroup{
		Key: g.Key, Count: g.Count, Errors: g.Errors, DurationMsSum: g.DurationMsSum,
		CPUTimeMsSum: g.CPUTimeMsSum, WaitTimeMsSum: g.WaitTimeMsSum,
		QueueWaitMsSum: g.QueueWaitMsSum, SuspendMsSum: g.SuspendMsSum,
		Sketch: g.Sketch.JSON(),
	}
}

// Group rebuilds a group from the wire.
func (g InternalGroup) Group() (Group, error) {
	sk, ok := g.Sketch.Sketch()
	if !ok {
		return Group{}, errors.Errorf("group %q carries a malformed sketch", g.Key)
	}
	return Group{Key: g.Key, Values: Values{
		Count: g.Count, Errors: g.Errors, DurationMsSum: g.DurationMsSum,
		CPUTimeMsSum: g.CPUTimeMsSum, WaitTimeMsSum: g.WaitTimeMsSum,
		QueueWaitMsSum: g.QueueWaitMsSum, SuspendMsSum: g.SuspendMsSum,
		Sketch: sk,
	}}, nil
}

// PKHash is the 64-bit hash of a call that keeps it from counting in both
// tiers: the first 8 bytes of sha256 over the URL form of the PK (02 §2.2,
// §2.9), big-endian.
func PKHash(pk model.PK) uint64 {
	sum := sha256.Sum256([]byte(pk.PathString()))
	return binary.BigEndian.Uint64(sum[:8])
}

// EncodeHashes renders hashes as the base64 of their 8-byte big-endian
// forms, the pk_hashes member of /internal/v1/stats.
func EncodeHashes(hashes []uint64) string {
	buf := make([]byte, 8*len(hashes))
	for i, h := range hashes {
		binary.BigEndian.PutUint64(buf[8*i:], h)
	}
	return base64.StdEncoding.EncodeToString(buf)
}

// DecodeHashes inverts EncodeHashes.
func DecodeHashes(text string) ([]uint64, error) {
	buf, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return nil, errors.Wrap(err, "decode pk_hashes")
	}
	if len(buf)%8 != 0 {
		return nil, errors.Errorf("pk_hashes holds %d bytes, not a multiple of 8", len(buf))
	}
	out := make([]uint64, len(buf)/8)
	for i := range out {
		out[i] = binary.BigEndian.Uint64(buf[8*i:])
	}
	return out, nil
}

// HashSet is a sorted set of PK hashes: 8 bytes per call, the size the read
// memory budget charges for it (02 §2.9).
type HashSet []uint64

// NewHashSet sorts the union of the hash lists into a set. It sorts in place
// when given one list.
func NewHashSet(lists ...[]uint64) HashSet {
	var all []uint64
	if len(lists) == 1 {
		all = lists[0]
	} else {
		n := 0
		for _, l := range lists {
			n += len(l)
		}
		all = make([]uint64, 0, n)
		for _, l := range lists {
			all = append(all, l...)
		}
	}
	slices.Sort(all)
	return HashSet(slices.Compact(all))
}

// Contains reports whether h is in the set.
func (s HashSet) Contains(h uint64) bool {
	_, ok := slices.BinarySearch(s, h)
	return ok
}
