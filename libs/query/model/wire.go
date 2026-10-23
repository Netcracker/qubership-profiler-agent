package model

import (
	"math"
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
		QueueWaitMs     int32               `json:"queue_wait_ms"`
		SuspendMs       int32               `json:"suspend_ms"`
		ChildCalls      int32               `json:"child_calls"`
		Transactions    int32               `json:"transactions"`
		LogsGenerated   int64               `json:"logs_generated"`
		LogsWritten     int64               `json:"logs_written"`
		FileRead        int64               `json:"file_read"`
		FileWritten     int64               `json:"file_written"`
		NetRead         int64               `json:"net_read"`
		NetWritten      int64               `json:"net_written"`
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
		QueueWaitMs:    r.QueueWaitMs,
		SuspendMs:      r.SuspendMs,
		ChildCalls:     r.ChildCalls,
		Transactions:   r.Transactions,
		LogsGenerated:  r.LogsGenerated,
		LogsWritten:    r.LogsWritten,
		FileRead:       r.FileRead,
		FileWritten:    r.FileWritten,
		NetRead:        r.NetRead,
		NetWritten:     r.NetWritten,
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
		QueueWaitMs:    c.QueueWaitMs,
		SuspendMs:      c.SuspendMs,
		ChildCalls:     c.ChildCalls,
		Transactions:   c.Transactions,
		LogsGenerated:  c.LogsGenerated,
		LogsWritten:    c.LogsWritten,
		FileRead:       c.FileRead,
		FileWritten:    c.FileWritten,
		NetRead:        c.NetRead,
		NetWritten:     c.NetWritten,
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

// ParseCallsQuery validates the §2.3 filter parameters and turns them into
// conditions (§2.3.3). It is shared by the external /api/v1/calls and the
// internal /internal/v1/calls, which takes the same params (02 §3). The second
// return is an empty string on success and the 400 detail otherwise.
func ParseCallsQuery(params url.Values) (CallsQuery, string) {
	var q CallsQuery
	fromMs, toMs, errDetail := ParseWindow(params)
	if errDetail != "" {
		return q, errDetail
	}
	q.FromMs, q.ToMs = fromMs, toMs
	if pods := params["pod"]; len(pods) > 0 {
		for _, p := range pods {
			if strings.Count(p, "/") != 2 {
				return q, "pod must be <namespace>/<service>/<pod>: " + p
			}
		}
		q.Filters = append(q.Filters, PodTuplesCondition(pods...))
	}
	if method := params.Get("method"); method != "" {
		q.Filters = append(q.Filters, MethodContainsCondition(method))
	}
	if classes := params["retention_class"]; len(classes) > 0 {
		for _, class := range classes {
			if !IsRetentionClass(class) {
				return q, "unknown retention_class: " + class
			}
		}
		q.Filters = append(q.Filters, RetentionClassCondition(classes...))
	}
	for _, bound := range []struct {
		name string
		op   Op
	}{{"duration_min_ms", OpGe}, {"duration_max_ms", OpLe}} {
		raw := params.Get(bound.name)
		if raw == "" {
			continue
		}
		v, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || v < 0 {
			return q, bound.name + " must be a non-negative integer"
		}
		if v > 0 { // 0 means "no bound"
			q.Filters = append(q.Filters, DurationCondition(bound.op, v))
		}
	}
	if raw := params.Get("error_only"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return q, "error_only must be a boolean"
		}
		if v {
			q.Filters = append(q.Filters, ErrorCondition(true))
		}
	}
	for _, text := range params["filter"] {
		c, err := ParseCondition(text)
		if err == nil {
			_, err = compileCondition(c)
		}
		if err == nil {
			err = checkImplemented(c)
		}
		if err != nil {
			return q, "filter " + strconv.Quote(text) + ": " + err.Error()
		}
		q.Filters = append(q.Filters, c)
	}
	if _, err := q.Matcher(); err != nil {
		return q, err.Error()
	}
	order, err := ParseOrder(params.Get("order"))
	if err != nil {
		return q, err.Error()
	}
	q.Order = order
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

// Values renders the query as parameters of the internal
// /internal/v1/calls, which takes the same parameters as /api/v1/calls
// (02 §3). The legacy parameters carry what they can express, so a collector
// that predates the filter model still pushes it down; every other condition
// goes as a `filter` value. The parameters select a superset of the query's
// rows, not always the exact set: an older collector ignores `filter`, and
// contradicting conditions have no legacy form. query therefore re-checks
// every hot row with the full Matcher.
func (q CallsQuery) Values() url.Values {
	v := url.Values{}
	v.Set("from", strconv.FormatInt(q.FromMs, 10))
	v.Set("to", strconv.FormatInt(q.ToMs, 10))
	if tuples, ok := q.PodTuples(); ok && len(tuples) > 0 {
		v["pod"] = tuples
	}
	for _, c := range q.Filters {
		if c.Field == "method" && c.Op == OpMatch && len(c.Values) == 1 {
			if text, ok := regexLiteral(c.Values[0]); ok && text != "" {
				v.Set("method", text)
				break
			}
		}
	}
	minMs, maxMs := q.DurationBounds()
	if minMs > 0 && minMs <= math.MaxInt32 {
		v.Set("duration_min_ms", strconv.FormatInt(minMs, 10))
	}
	if maxMs >= 1 && maxMs <= math.MaxInt32 { // 0 would mean "no bound"
		v.Set("duration_max_ms", strconv.FormatInt(maxMs, 10))
	}
	if q.ErrorRequired() {
		v.Set("error_only", "true")
	}
	if classes, ok := q.RetentionClassSet(); ok && len(classes) > 0 {
		v["retention_class"] = classes
	}
	for _, c := range q.Filters {
		if !hasLegacyForm(c) {
			v.Add("filter", c.String())
		}
	}
	if q.Order.ByDuration() {
		v.Set("order", q.Order.String())
	}
	return v
}

// hasLegacyForm reports whether Values carries the condition exactly through
// a legacy parameter. A method condition never qualifies: the legacy `method`
// parameter carries at most one literal word, so every method condition also
// goes as a `filter` value.
func hasLegacyForm(c Condition) bool {
	switch c.Field {
	case FieldPodTuple, "duration":
		return true
	case "error":
		return c.Op == OpEq && len(c.Values) == 1 && c.Values[0] == "true"
	case "retention_class":
		return c.Op == OpEq || c.Op == OpIn
	}
	return false
}

// regexLiteral returns the text a pattern matches when it has no
// regular-expression syntax other than escaped syntax characters, as
// regexp.QuoteMeta writes it.
func regexLiteral(pattern string) (string, bool) {
	const special = `\.+*?()|[]{}^$`
	var out strings.Builder
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		if ch == '\\' {
			if i+1 == len(pattern) || !strings.ContainsRune(special, rune(pattern[i+1])) {
				return "", false
			}
			out.WriteByte(pattern[i+1])
			i++
			continue
		}
		if strings.ContainsRune(special, rune(ch)) {
			return "", false
		}
		out.WriteByte(ch)
	}
	return out.String(), true
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
