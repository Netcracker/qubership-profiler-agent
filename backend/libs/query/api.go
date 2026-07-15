package query

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/labstack/echo/v4"
)

// problem is an RFC 7807 body (02 §8) with the §2.3.2 guard extensions.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`

	SuggestedFilters []string         `json:"suggested_filters,omitempty"`
	EstimatedFiles   *int             `json:"estimated_files,omitempty"`
	EstimatedBytes   *int64           `json:"estimated_bytes,omitempty"`
	ByClass          map[string]int64 `json:"by_class,omitempty"`
}

// callsResponse is the /calls page (02 §2.3).
type callsResponse struct {
	Calls          []callJSON `json:"calls"`
	NextCursor     *string    `json:"next_cursor"`
	Partial        bool       `json:"partial"`
	PartialReasons []string   `json:"partial_reasons"`
}

// callJSON is one §2.3 row. trace_blob_size is 0 for a truncated row and
// null otherwise: CallV2 (01 §5.2) has no blob-size column and the list path
// never reads trace_blob, so the cold tier cannot know the size — see the
// stage1-progress.md open issue proposing an additive column.
type callJSON struct {
	PK              model.PK            `json:"pk"`
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

// podsResponse is the /pods body. 02 §2.7 pins the tuple set but not the
// JSON shape; the member names follow the pods/v1 manifest fields (decision
// in stage1-progress.md).
type podsResponse struct {
	Pods           []model.PodTuple `json:"pods"`
	Partial        bool             `json:"partial"`
	PartialReasons []string         `json:"partial_reasons"`
}

func (s *Service) routes(e *echo.Echo) {
	e.GET("/api/v1/calls", s.handleCalls)
	e.GET("/api/v1/pods", s.handlePods)
}

func sendProblem(c echo.Context, p problem) error {
	if p.Type == "" {
		p.Type = "about:blank"
	}
	c.Response().Header().Set(echo.HeaderContentType, "application/problem+json")
	c.Response().WriteHeader(p.Status)
	return json.NewEncoder(c.Response()).Encode(p)
}

func badRequest(c echo.Context, detail string) error {
	return sendProblem(c, problem{Title: "invalid request", Status: http.StatusBadRequest, Detail: detail})
}

// handleCalls serves GET /api/v1/calls (02 §2.3): validate or thaw the
// frozen query, run the wide-query guard on page 1, discover, scan, merge,
// and mint the next cursor.
func (s *Service) handleCalls(c echo.Context) error {
	ctx := c.Request().Context()
	params := c.QueryParams()

	var q model.CallsQuery
	var after *model.Position
	firstPage := !params.Has("cursor")
	if firstPage {
		parsed, errDetail := parseCallsQuery(params)
		if errDetail != "" {
			return badRequest(c, errDetail)
		}
		q = parsed
		if rej := guardSpan(q, s.cfg.WideRangeLimit); rej != nil {
			return s.sendGuardRejection(c, rej)
		}
	} else {
		tok, err := decodeCursor(params.Get("cursor"), s.cfg.CursorTTL)
		if err != nil {
			return badRequest(c, err.Error())
		}
		// Pages 2..N run against the frozen query; re-sent filters must match
		// it or the request is rejected (02 §2.3.1).
		if detail := frozenQueryMismatch(tok.Query, params); detail != "" {
			return badRequest(c, detail)
		}
		q = tok.Query
		after = &tok.Pos
	}

	limit, errDetail := parseLimit(params.Get("limit"), s.cfg)
	if errDetail != "" {
		return badRequest(c, errDetail)
	}

	discovery, err := s.cold.Discover(ctx, q)
	if err != nil {
		return err
	}
	if firstPage {
		if rej := guardCost(q, discovery.Files, s.cfg.MaxScanFiles, s.cfg.MaxScanBytes); rej != nil {
			return s.sendGuardRejection(c, rej)
		}
	}
	if discovery.Prefixes > 0 && discovery.FailedPrefixes == discovery.Prefixes {
		// The cold LIST is the only wired source; nothing produced data (02 §8).
		return sendProblem(c, problem{Title: "no data source available", Status: http.StatusGatewayTimeout,
			Detail: strings.Join(discovery.PartialReasons, "; ")})
	}

	scan, err := s.cold.Calls(ctx, discovery, q, after, limit)
	if err != nil {
		return err
	}

	resp := callsResponse{
		Calls:          make([]callJSON, 0, len(scan.Rows)),
		PartialReasons: append(discovery.PartialReasons, scan.PartialReasons...),
	}
	resp.Partial = len(resp.PartialReasons) > 0
	if resp.PartialReasons == nil {
		resp.PartialReasons = []string{}
	}
	for _, row := range scan.Rows {
		resp.Calls = append(resp.Calls, toCallJSON(row))
	}
	if scan.More && len(scan.Rows) > 0 {
		cur := encodeCursor(q, scan.Rows[len(scan.Rows)-1].Position())
		resp.NextCursor = &cur
	}
	return c.JSON(http.StatusOK, resp)
}

// handlePods serves GET /api/v1/pods (02 §2.7); the cold set comes from the
// pods/v1 manifests, never from parquet.
func (s *Service) handlePods(c echo.Context) error {
	fromMs, toMs, errDetail := parseWindow(c.QueryParams())
	if errDetail != "" {
		return badRequest(c, errDetail)
	}
	res, err := s.cold.Pods(c.Request().Context(), fromMs, toMs)
	if err != nil {
		return err
	}
	if res.Prefixes > 0 && res.FailedPrefixes == res.Prefixes {
		return sendProblem(c, problem{Title: "no data source available", Status: http.StatusGatewayTimeout,
			Detail: strings.Join(res.PartialReasons, "; ")})
	}
	resp := podsResponse{Pods: res.Pods, Partial: len(res.PartialReasons) > 0, PartialReasons: res.PartialReasons}
	if resp.Pods == nil {
		resp.Pods = []model.PodTuple{}
	}
	if resp.PartialReasons == nil {
		resp.PartialReasons = []string{}
	}
	return c.JSON(http.StatusOK, resp)
}

func (s *Service) sendGuardRejection(c echo.Context, rej *guardRejection) error {
	p := problem{
		Title:            "query too wide",
		Status:           http.StatusBadRequest,
		Detail:           rej.Detail,
		SuggestedFilters: rej.SuggestedFilters,
	}
	if rej.HasEstimate {
		p.EstimatedFiles = &rej.EstimatedFiles
		p.EstimatedBytes = &rej.EstimatedBytes
		p.ByClass = rej.ByClass
	}
	return sendProblem(c, p)
}

func toCallJSON(row model.CallRow) callJSON {
	out := callJSON{
		PK:             row.PK,
		TsMs:           row.TsMs,
		DurationMs:     row.DurationMs,
		Method:         row.Method,
		ThreadName:     row.ThreadName,
		CpuTimeMs:      row.CpuTimeMs,
		WaitTimeMs:     row.WaitTimeMs,
		MemoryUsed:     row.MemoryUsed,
		ChildCalls:     row.ChildCalls,
		ErrorFlag:      row.ErrorFlag,
		RetentionClass: row.RetentionClass,
		Params:         row.Params,
	}
	if out.Params == nil {
		out.Params = map[string][]string{}
	}
	if row.TruncatedReason != "" {
		reason := row.TruncatedReason
		out.TruncatedReason = &reason
		zero := int64(0)
		out.TraceBlobSize = &zero // 02 §2.3: 0 when the blob was dropped
	}
	return out
}

// parseCallsQuery validates the page-1 parameters (02 §2.3).
func parseCallsQuery(params map[string][]string) (model.CallsQuery, string) {
	get := func(name string) string {
		if vs := params[name]; len(vs) > 0 {
			return vs[0]
		}
		return ""
	}
	var q model.CallsQuery
	fromMs, toMs, errDetail := parseWindow(params)
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
	q.Method = get("method")
	for _, class := range params["retention_class"] {
		if !model.IsRetentionClass(class) {
			return q, "unknown retention_class: " + class
		}
		q.RetentionClasses = append(q.RetentionClasses, class)
	}
	for name, dst := range map[string]*int32{"duration_min_ms": &q.DurationMinMs, "duration_max_ms": &q.DurationMaxMs} {
		if raw := get(name); raw != "" {
			v, err := strconv.ParseInt(raw, 10, 32)
			if err != nil || v < 0 {
				return q, name + " must be a non-negative integer"
			}
			*dst = int32(v)
		}
	}
	if raw := get("error_only"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return q, "error_only must be a boolean"
		}
		q.ErrorOnly = v
	}
	return q, ""
}

func parseWindow(params map[string][]string) (int64, int64, string) {
	get := func(name string) string {
		if vs := params[name]; len(vs) > 0 {
			return vs[0]
		}
		return ""
	}
	fromRaw, toRaw := get("from"), get("to")
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

func parseLimit(raw string, cfg Config) (int, string) {
	if raw == "" {
		return cfg.DefaultLimit, ""
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, "limit must be a positive integer"
	}
	if v > cfg.MaxLimit {
		return 0, "limit must not exceed " + strconv.Itoa(cfg.MaxLimit)
	}
	return v, ""
}

// frozenQueryMismatch compares re-sent filter parameters against the frozen
// query (02 §2.3.1): parameters the client omits fall back to the frozen
// values; parameters it re-sends must match them exactly.
func frozenQueryMismatch(frozen model.CallsQuery, params map[string][]string) string {
	resent, errDetail := parseResent(frozen, params)
	if errDetail != "" {
		return errDetail
	}
	if !callsQueryEqual(frozen, resent) {
		return "re-sent filters do not match the query frozen in the cursor; restart from page 1"
	}
	return ""
}

// parseResent overlays the parameters present in the request onto the frozen
// query, so an omitted parameter never counts as a mismatch.
func parseResent(frozen model.CallsQuery, params map[string][]string) (model.CallsQuery, string) {
	out := frozen
	get := func(name string) string {
		if vs := params[name]; len(vs) > 0 {
			return vs[0]
		}
		return ""
	}
	if raw := get("from"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return out, "from must be Unix ms"
		}
		out.FromMs = v
	}
	if raw := get("to"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return out, "to must be Unix ms"
		}
		out.ToMs = v
	}
	if vs, ok := params["pod"]; ok {
		out.Pods = vs
	}
	if vs, ok := params["method"]; ok && len(vs) > 0 {
		out.Method = vs[0]
	}
	if vs, ok := params["retention_class"]; ok {
		out.RetentionClasses = vs
	}
	if raw := get("duration_min_ms"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return out, "duration_min_ms must be an integer"
		}
		out.DurationMinMs = int32(v)
	}
	if raw := get("duration_max_ms"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return out, "duration_max_ms must be an integer"
		}
		out.DurationMaxMs = int32(v)
	}
	if raw := get("error_only"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return out, "error_only must be a boolean"
		}
		out.ErrorOnly = v
	}
	return out, ""
}

func callsQueryEqual(a, b model.CallsQuery) bool {
	if a.FromMs != b.FromMs || a.ToMs != b.ToMs || a.Method != b.Method ||
		a.DurationMinMs != b.DurationMinMs || a.DurationMaxMs != b.DurationMaxMs ||
		a.ErrorOnly != b.ErrorOnly {
		return false
	}
	return stringsEqual(a.Pods, b.Pods) && stringsEqual(a.RetentionClasses, b.RetentionClasses)
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
