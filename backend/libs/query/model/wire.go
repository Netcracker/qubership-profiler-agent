package model

import (
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type (
	// CallJSON is the wire form of one /calls row (02 §2.3). The internal
	// hot-read API returns the same shape as /api/v1 (02 §3), so both tiers
	// render and parse this one struct and cannot drift apart.
	// trace_blob_size is null when the tier cannot know the size without
	// reading the blob (both the cold list projection and the hot index), and
	// 0 for a truncated row, as §2.3 pins.
	CallJSON struct {
		PK              PK                  `json:"pk"`
		TsMs            int64               `json:"ts_ms"`
		DurationMs      int32               `json:"duration_ms"`
		Method          string              `json:"method"`
		ThreadName      string              `json:"thread_name"`
		CpuTimeMs       int64               `json:"cpu_time_ms"`
		WaitTimeMs      int64               `json:"wait_time_ms"`
		MemoryUsed      int64               `json:"memory_used"`
		ChildCalls      int32               `json:"child_calls"`
		ErrorFlag       bool                `json:"error_flag"`
		RetentionClass  string              `json:"retention_class"`
		Params          map[string][]string `json:"params"`
		TraceBlobSize   *int64              `json:"trace_blob_size"`
		TruncatedReason *string             `json:"truncated_reason"`
	}

	// PodEntry is one /pods row (02 §2.7): the identity tuple plus the time
	// bounds of the data behind it. Hot and cold produce this same shape, so
	// the tier union needs no reshaping.
	PodEntry struct {
		PodTuple
		TimeMinMs int64 `json:"time_min_ms"`
		TimeMaxMs int64 `json:"time_max_ms"`
	}
)

// JSON renders the row for the wire.
func (r CallRow) JSON() CallJSON {
	out := CallJSON{
		PK:             r.PK,
		TsMs:           r.TsMs,
		DurationMs:     r.DurationMs,
		Method:         r.Method,
		ThreadName:     r.ThreadName,
		CpuTimeMs:      r.CpuTimeMs,
		WaitTimeMs:     r.WaitTimeMs,
		MemoryUsed:     r.MemoryUsed,
		ChildCalls:     r.ChildCalls,
		ErrorFlag:      r.ErrorFlag,
		RetentionClass: r.RetentionClass,
		Params:         r.Params,
	}
	if out.Params == nil {
		out.Params = map[string][]string{}
	}
	if r.TruncatedReason != "" {
		reason := r.TruncatedReason
		out.TruncatedReason = &reason
		zero := int64(0)
		out.TraceBlobSize = &zero // 02 §2.3: 0 when the blob was dropped
	}
	return out
}

// Row rebuilds the merged row shape from the wire form, tagging its tier.
func (c CallJSON) Row(tier Tier) CallRow {
	row := CallRow{
		PK:             c.PK,
		TsMs:           c.TsMs,
		DurationMs:     c.DurationMs,
		Method:         c.Method,
		ThreadName:     c.ThreadName,
		CpuTimeMs:      c.CpuTimeMs,
		WaitTimeMs:     c.WaitTimeMs,
		MemoryUsed:     c.MemoryUsed,
		ChildCalls:     c.ChildCalls,
		ErrorFlag:      c.ErrorFlag,
		RetentionClass: c.RetentionClass,
		Params:         c.Params,
		Tier:           tier,
	}
	if c.TruncatedReason != nil {
		row.TruncatedReason = *c.TruncatedReason
	}
	return row
}

// PathString renders the §2.2 URL form of the PK:
// <ns>:<svc>:<pod>:<restartMs>:<file>:<off>:<rec>. Kubernetes names cannot
// contain ':', so the segments split unambiguously; percent-encoding is the
// HTTP layer's job.
func (p PK) PathString() string {
	return strings.Join([]string{
		p.PodNamespace, p.PodService, p.PodName,
		strconv.FormatInt(p.RestartTimeMs, 10),
		strconv.FormatInt(int64(p.TraceFileIndex), 10),
		strconv.FormatInt(int64(p.BufferOffset), 10),
		strconv.FormatInt(int64(p.RecordIndex), 10),
	}, ":")
}

// ParsePKPath inverts PathString.
func ParsePKPath(s string) (PK, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 7 {
		return PK{}, errors.Errorf("pk %q: expected 7 colon-separated components (02 §2.2)", s)
	}
	nums := make([]int64, 4)
	for i, part := range parts[3:] {
		v, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return PK{}, errors.Wrapf(err, "pk %q: component %d is not an integer", s, i+4)
		}
		nums[i] = v
	}
	return PK{
		PodNamespace: parts[0], PodService: parts[1], PodName: parts[2],
		RestartTimeMs:  nums[0],
		TraceFileIndex: int32(nums[1]),
		BufferOffset:   int32(nums[2]),
		RecordIndex:    int32(nums[3]),
	}, nil
}

// ParsePodRestartPath decodes the §2.6 pod-restart path segment
// <ns>:<svc>:<pod>:<restartMs>.
func ParsePodRestartPath(s string) (PodTuple, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 4 {
		return PodTuple{}, errors.Errorf("pod-restart %q: expected 4 colon-separated components (02 §2.6)", s)
	}
	ms, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return PodTuple{}, errors.Wrapf(err, "pod-restart %q: bad restart time", s)
	}
	return PodTuple{Namespace: parts[0], Service: parts[1], Pod: parts[2], RestartTimeMs: ms}, nil
}

// ParseCallsQuery validates the §2.3 filter parameters. It is shared by the
// external /api/v1/calls and the internal /internal/v1/calls, which takes the
// same params (02 §3). The second return is an empty string on success and
// the 400 detail otherwise.
func ParseCallsQuery(params url.Values) (CallsQuery, string) {
	var q CallsQuery
	fromMs, toMs, errDetail := ParseWindow(params)
	if errDetail != "" {
		return q, errDetail
	}
	q.FromMs, q.ToMs = fromMs, toMs
	q.Pods = append(q.Pods, params["pod"]...)
	for _, p := range q.Pods {
		if strings.Count(p, "/") != 2 {
			return q, "pod must be <namespace>/<service>/<pod>: " + p
		}
	}
	q.Method = params.Get("method")
	for _, class := range params["retention_class"] {
		if !IsRetentionClass(class) {
			return q, "unknown retention_class: " + class
		}
		q.RetentionClasses = append(q.RetentionClasses, class)
	}
	for name, dst := range map[string]*int32{"duration_min_ms": &q.DurationMinMs, "duration_max_ms": &q.DurationMaxMs} {
		if raw := params.Get(name); raw != "" {
			v, err := strconv.ParseInt(raw, 10, 32)
			if err != nil || v < 0 {
				return q, name + " must be a non-negative integer"
			}
			*dst = int32(v)
		}
	}
	if raw := params.Get("error_only"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return q, "error_only must be a boolean"
		}
		q.ErrorOnly = v
	}
	return q, ""
}

// ParseWindow validates the required from/to pair (02 §2.3, §2.7).
func ParseWindow(params url.Values) (int64, int64, string) {
	fromRaw, toRaw := params.Get("from"), params.Get("to")
	if fromRaw == "" || toRaw == "" {
		return 0, 0, "from and to are required (Unix ms)"
	}
	fromMs, err := strconv.ParseInt(fromRaw, 10, 64)
	if err != nil {
		return 0, 0, "from must be Unix ms"
	}
	toMs, err := strconv.ParseInt(toRaw, 10, 64)
	if err != nil {
		return 0, 0, "to must be Unix ms"
	}
	if toMs <= fromMs {
		return 0, 0, "to must be greater than from"
	}
	return fromMs, toMs, ""
}

// Values inverts ParseCallsQuery for the fan-out client: the internal
// /internal/v1/calls takes the same parameters as /api/v1/calls (02 §3).
func (q CallsQuery) Values() url.Values {
	v := url.Values{}
	v.Set("from", strconv.FormatInt(q.FromMs, 10))
	v.Set("to", strconv.FormatInt(q.ToMs, 10))
	for _, p := range q.Pods {
		v.Add("pod", p)
	}
	if q.Method != "" {
		v.Set("method", q.Method)
	}
	if q.DurationMinMs > 0 {
		v.Set("duration_min_ms", strconv.FormatInt(int64(q.DurationMinMs), 10))
	}
	if q.DurationMaxMs > 0 {
		v.Set("duration_max_ms", strconv.FormatInt(int64(q.DurationMaxMs), 10))
	}
	if q.ErrorOnly {
		v.Set("error_only", "true")
	}
	for _, class := range q.RetentionClasses {
		v.Add("retention_class", class)
	}
	return v
}

// UnionPods merges pod entries from several sources into the §2.7 union: one
// entry per identity tuple, time bounds widened to cover every source (a
// pod-restart can surface from the hot tier and from one cold manifest per
// UTC day). The result is sorted by the identity tuple for a stable order.
func UnionPods(groups ...[]PodEntry) []PodEntry {
	merged := map[PodTuple]PodEntry{}
	for _, group := range groups {
		for _, e := range group {
			cur, ok := merged[e.PodTuple]
			if !ok {
				merged[e.PodTuple] = e
				continue
			}
			if e.TimeMinMs < cur.TimeMinMs {
				cur.TimeMinMs = e.TimeMinMs
			}
			if e.TimeMaxMs > cur.TimeMaxMs {
				cur.TimeMaxMs = e.TimeMaxMs
			}
			merged[e.PodTuple] = cur
		}
	}
	out := make([]PodEntry, 0, len(merged))
	for _, e := range merged {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		if a.Pod != b.Pod {
			return a.Pod < b.Pod
		}
		return a.RestartTimeMs < b.RestartTimeMs
	})
	return out
}
