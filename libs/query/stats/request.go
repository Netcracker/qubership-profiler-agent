package stats

import (
	"maps"
	"math"
	"net/url"
	"slices"
	"strconv"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
)

// Order ranks the groups of a field grouping (02 §2.9).
type Order string

const (
	OrderCountDesc       Order = "count_desc"
	OrderDurationSumDesc Order = "duration_sum_desc"
)

// GroupByTime is the time grouping; every other grouping names a field.
const GroupByTime = "time"

// Parameter bounds of 02 §2.9.
const (
	DefaultBuckets = 60
	MaxBuckets     = 1000
	DefaultLimit   = 20
	MaxLimit       = 1000
	// DefaultMaxGroups is PROFILER_STATS_MAX_GROUPS (02 §9).
	DefaultMaxGroups = 10_000
)

// The parameters of the internal endpoint only (02 §3).
const (
	paramBucketOrigin = "bucket_origin_ms"
	paramBucketMs     = "bucket_ms"
	paramMaxGroups    = "max_groups"
)

// Request is one validated /stats request.
type Request struct {
	// Query is the window and the conditions, as for /calls.
	Query   model.CallsQuery
	GroupBy string
	// Buckets is the requested bucket count of a time grouping.
	Buckets int
	// Limit and Order apply to a field grouping.
	Limit int
	Order Order
}

// ParseRequest validates the parameters of GET /api/v1/stats (02 §2.9). The
// second return is empty on success and the 400 detail otherwise.
func ParseRequest(params url.Values) (Request, string) {
	if params.Has("cursor") {
		return Request{}, "cursor does not apply to /stats"
	}
	// order names a /stats ranking here, not a /calls order, so the
	// condition parser must not see it.
	callParams := maps.Clone(params)
	delete(callParams, "order")
	q, detail := model.ParseCallsQuery(callParams)
	if detail != "" {
		return Request{}, detail
	}
	r := Request{Query: q, GroupBy: params.Get("group_by")}
	switch {
	case r.GroupBy == "":
		return Request{}, "group_by is required: " + groupingList()
	case !slices.Contains(model.StatsGroupings, r.GroupBy):
		return Request{}, "group_by=" + r.GroupBy + " is not implemented; this build groups by " + groupingList()
	}
	if r.GroupBy == GroupByTime {
		for _, name := range []string{"limit", "order"} {
			if params.Has(name) {
				return Request{}, name + " applies to a field grouping, not to group_by=time"
			}
		}
		r.Buckets = DefaultBuckets
		if params.Has("buckets") {
			v, err := strconv.Atoi(params.Get("buckets"))
			if err != nil || v < 1 || v > MaxBuckets {
				return Request{}, "buckets must be an integer in 1.." + strconv.Itoa(MaxBuckets)
			}
			r.Buckets = v
		}
		if _, ok := bucketMs(q.FromMs, q.ToMs, r.Buckets); !ok {
			return Request{}, "the window is too wide to split into buckets"
		}
		return r, ""
	}
	if params.Has("buckets") {
		return Request{}, "buckets applies to group_by=time only"
	}
	r.Limit = DefaultLimit
	if params.Has("limit") {
		v, err := strconv.Atoi(params.Get("limit"))
		if err != nil || v < 1 || v > MaxLimit {
			return Request{}, "limit must be an integer in 1.." + strconv.Itoa(MaxLimit)
		}
		r.Limit = v
	}
	r.Order = OrderCountDesc
	if params.Has("order") {
		switch o := Order(params.Get("order")); o {
		case OrderCountDesc, OrderDurationSumDesc:
			r.Order = o
		default:
			return Request{}, "order must be count_desc or duration_sum_desc"
		}
	}
	return r, ""
}

func groupingList() string {
	out := ""
	for i, g := range model.StatsGroupings {
		if i > 0 {
			out += ", "
		}
		out += g
	}
	return out
}

// bucketMs is max(1, ceil((to − from) / buckets)) (02 §2.9). The span is
// taken in uint64, where to − from cannot overflow for to > from; a bucket
// wider than int64 reports false.
func bucketMs(fromMs, toMs int64, buckets int) (int64, bool) {
	span := uint64(toMs) - uint64(fromMs)
	ms := span / uint64(buckets)
	if span%uint64(buckets) != 0 {
		ms++
	}
	if ms > math.MaxInt64 {
		return 0, false
	}
	return max(1, int64(ms)), true
}

// Grouping returns how the request places calls into groups. For a time
// grouping the buckets start at the window's from.
func (r Request) Grouping() Grouping {
	if r.GroupBy != GroupByTime {
		return Grouping{Field: r.GroupBy}
	}
	ms, _ := bucketMs(r.Query.FromMs, r.Query.ToMs, r.Buckets)
	return Grouping{Field: GroupByTime, OriginMs: r.Query.FromMs, BucketMs: ms}
}

// InternalValues renders the request as parameters of /internal/v1/stats
// for the rows of [fromMs, to): the window of the request narrowed to the
// hot part (02 §2.9), with the time buckets still placed on the request's
// own grid.
func (r Request) InternalValues(fromMs int64, maxGroups int) url.Values {
	q := r.Query
	q.FromMs = fromMs
	v := q.Values()
	v.Set("group_by", r.GroupBy)
	if r.GroupBy == GroupByTime {
		g := r.Grouping()
		v.Set(paramBucketOrigin, strconv.FormatInt(g.OriginMs, 10))
		v.Set(paramBucketMs, strconv.FormatInt(g.BucketMs, 10))
	} else {
		v.Set("order", string(r.Order))
	}
	v.Set(paramMaxGroups, strconv.Itoa(maxGroups))
	return v
}

// InternalRequest is one validated /internal/v1/stats request.
type InternalRequest struct {
	Query     model.CallsQuery
	Grouping  Grouping
	Order     Order
	MaxGroups int
}

// ParseInternalRequest validates the parameters of /internal/v1/stats (02
// §3): those of /api/v1/stats, where limit is ignored because a replica
// returns every group, plus bucket_origin_ms, bucket_ms, and max_groups.
func ParseInternalRequest(params url.Values) (InternalRequest, string) {
	ext := url.Values{}
	for name, vs := range params {
		switch name {
		case paramBucketOrigin, paramBucketMs, paramMaxGroups, "limit":
		default:
			ext[name] = vs
		}
	}
	if params.Has(paramBucketMs) {
		// The grid comes from the caller; buckets would only re-derive it.
		ext.Del("buckets")
	}
	r, detail := ParseRequest(ext)
	if detail != "" {
		return InternalRequest{}, detail
	}
	out := InternalRequest{Query: r.Query, Grouping: r.Grouping(), Order: r.Order, MaxGroups: DefaultMaxGroups}
	if r.GroupBy == GroupByTime {
		if raw := params.Get(paramBucketMs); raw != "" {
			v, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || v < 1 {
				return InternalRequest{}, paramBucketMs + " must be a positive integer"
			}
			out.Grouping.BucketMs = v
		}
		if raw := params.Get(paramBucketOrigin); raw != "" {
			v, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || v > r.Query.FromMs {
				return InternalRequest{}, paramBucketOrigin + " must be Unix ms at or before from"
			}
			out.Grouping.OriginMs = v
		}
	}
	if raw := params.Get(paramMaxGroups); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 {
			return InternalRequest{}, paramMaxGroups + " must be a positive integer"
		}
		out.MaxGroups = v
	}
	return out, ""
}
