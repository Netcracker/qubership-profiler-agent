package query

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
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
	Calls          []model.CallJSON `json:"calls"`
	NextCursor     *string          `json:"next_cursor"`
	Partial        bool             `json:"partial"`
	PartialReasons []string         `json:"partial_reasons"`
}

// podsResponse is the /pods body: the §2.7 union of live (hot) and closed
// (cold-manifest) pod-restarts on the entry shape both tiers share.
type podsResponse struct {
	Pods           []model.PodEntry `json:"pods"`
	Partial        bool             `json:"partial"`
	PartialReasons []string         `json:"partial_reasons"`
}

// configResponse is the /config body: deployment-specific values the UI has
// no other way to learn, currently just the dumps-collector link-out base
// (PR 708 review #18). Empty fields mean the feature they back is
// unavailable in this deployment, not an error.
type configResponse struct {
	DumpsCollectorURL string `json:"dumps_collector_url"`
}

func (s *Service) routes(e *echo.Echo) {
	e.GET("/api/v1/calls", s.handleCalls)
	e.GET("/api/v1/pods", s.handlePods)
	e.GET("/api/v1/config", s.handleConfig)
	e.GET("/api/v1/calls/:pk/trace", s.handleCallTrace)
	// gzip is per-route: /tree wants it (02 §2.5.5), while /trace serves raw
	// bytes with Range support, which the middleware would break.
	e.GET("/api/v1/calls/:pk/tree", s.handleCallTree, middleware.Gzip())
	if s.ui != nil {
		// The embedded single-page app (07 §6); /api/v1, /metrics, and the
		// health routes stay untouched.
		e.GET("/ui", s.handleUI, middleware.Gzip())
		e.GET("/ui/*", s.handleUI, middleware.Gzip())
	}
}

// handleConfig serves GET /api/v1/config: static, deployment-specific values
// the UI cannot derive on its own.
func (s *Service) handleConfig(c echo.Context) error {
	return c.JSON(http.StatusOK, configResponse{DumpsCollectorURL: s.cfg.DumpsCollectorURL})
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

func gatewayTimeout(c echo.Context, reasons []string) error {
	return sendProblem(c, problem{Title: "no data source available", Status: http.StatusGatewayTimeout,
		Detail: strings.Join(reasons, "; ")})
}

// handleCalls serves GET /api/v1/calls (02 §2.3): validate or thaw the frozen
// query, run the wide-query guard on page 1, then fan out to both tiers —
// every hot replica plus the cutoff-bounded cold LIST — and k-way merge with
// cold-preferred PK dedup (§2.3.1, §4.3, §6). The fan-out is re-issued whole
// on every page; the cursor carries the only cross-page state.
func (s *Service) handleCalls(c echo.Context) error {
	ctx := c.Request().Context()
	params := c.QueryParams()

	var q model.CallsQuery
	var after *model.Position
	firstPage := !params.Has("cursor")
	if firstPage {
		parsed, errDetail := model.ParseCallsQuery(params)
		if errDetail != "" {
			return badRequest(c, errDetail)
		}
		q = parsed
		if rej := s.guardSpan(q, s.cfg.WideRangeLimit); rej != nil {
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

	tier := s.resolveHotTier(ctx)
	partial := append([]string{}, tier.partialReasons...)
	succeeded, failed := 0, 0
	if tier.resolveFailed {
		failed++
	}

	// Cold tier, bounded by the dynamic cutoff (02 §4.3). A window that ends
	// inside every replica's hot coverage skips the LIST entirely.
	var runs [][]model.CallRow
	more := false
	if coldTo := tier.coldToMs(q, s.cfg.OverlapMargin.Milliseconds()); coldTo > q.FromMs {
		coldQ := q
		coldQ.ToMs = coldTo
		discovery, err := s.cold.Discover(ctx, coldQ)
		if err != nil {
			return err
		}
		if firstPage {
			if rej := guardCost(q, discovery.Files, s.cfg.MaxScanFiles, s.cfg.MaxScanBytes); rej != nil {
				return s.sendGuardRejection(c, rej)
			}
		}
		scan, err := s.cold.Calls(ctx, discovery, coldQ, after, limit)
		if err != nil {
			return err
		}
		partial = append(partial, discovery.PartialReasons...)
		partial = append(partial, scan.PartialReasons...)
		if discovery.Prefixes > 0 && discovery.FailedPrefixes == discovery.Prefixes {
			failed++
		} else {
			succeeded++
		}
		if len(scan.Rows) > 0 {
			runs = append(runs, scan.Rows)
		}
		more = more || scan.More
	}

	// Hot tier: one sorted run per healthy replica (02 §3, §7.2). A replica
	// that filled its page may hold more rows past it, so it keeps next_cursor
	// alive; the §2.3.1 termination tolerates the resulting empty last page.
	hotRuns, hotOK, hotFail, hotReasons := s.hotCalls(ctx, tier, q, after, limit)
	succeeded += hotOK
	failed += hotFail
	partial = append(partial, hotReasons...)
	for _, run := range hotRuns {
		if len(run) == limit {
			more = true
		}
	}
	runs = append(runs, hotRuns...)

	// §8: 504 only when every attempted source failed — no data at all.
	if succeeded == 0 && failed > 0 {
		return gatewayTimeout(c, partial)
	}

	rows, mergeMore := model.MergeRuns(runs, limit)
	more = more || mergeMore

	resp := callsResponse{
		Calls:          make([]model.CallJSON, 0, len(rows)),
		Partial:        len(partial) > 0,
		PartialReasons: partial,
	}
	s.metrics.countPartial(resp.Partial)
	if resp.PartialReasons == nil {
		resp.PartialReasons = []string{}
	}
	for _, row := range rows {
		resp.Calls = append(resp.Calls, row.JSON())
	}
	if more && len(rows) > 0 {
		cur := encodeCursor(q, rows[len(rows)-1].Position())
		resp.NextCursor = &cur
	}
	return c.JSON(http.StatusOK, resp)
}

// handlePods serves GET /api/v1/pods (02 §2.7): the union of live
// pod-restarts from the hot replicas and closed ones from the pods/v1
// manifests, merged on the identity tuple with widened time bounds.
func (s *Service) handlePods(c echo.Context) error {
	ctx := c.Request().Context()
	fromMs, toMs, errDetail := model.ParseWindow(c.QueryParams())
	if errDetail != "" {
		return badRequest(c, errDetail)
	}

	hotPods, hotOK, hotFail, hotReasons := s.hotPods(ctx, fromMs, toMs)

	res, err := s.cold.Pods(ctx, fromMs, toMs)
	if err != nil {
		return err
	}
	succeeded, failed := hotOK, hotFail
	if res.Prefixes > 0 && res.FailedPrefixes == res.Prefixes {
		failed++
	} else {
		succeeded++
	}
	partial := append(append([]string{}, hotReasons...), res.PartialReasons...)
	if succeeded == 0 && failed > 0 {
		return gatewayTimeout(c, partial)
	}

	resp := podsResponse{
		Pods:           model.UnionPods(res.Pods, hotPods),
		Partial:        len(partial) > 0,
		PartialReasons: partial,
	}
	s.metrics.countPartial(resp.Partial)
	if resp.PartialReasons == nil {
		resp.PartialReasons = []string{}
	}
	return c.JSON(http.StatusOK, resp)
}

func (s *Service) sendGuardRejection(c echo.Context, rej *guardRejection) error {
	s.metrics.countGuardRejection(rej.Layer)
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
