package query

import (
	"errors"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/httpproblem"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// problem is an RFC 7807 body (02 §8) with the §2.3.2 guard extensions. The
// embedded envelope marshals flat, so the extensions sit beside its members.
type problem struct {
	httpproblem.Problem

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
	// FilterCapabilities is what this build implements of the filter model
	// (02 §2.3.4), so the UI can disable the rest instead of probing.
	FilterCapabilities model.FilterCapabilities `json:"filter_capabilities"`
}

func (s *Service) routes(e *echo.Echo) {
	e.GET("/api/v1/calls", s.handleCalls)
	e.GET("/api/v1/pods", s.handlePods)
	e.GET("/api/v1/stats", s.handleStats)
	e.GET("/api/v1/config", s.handleConfig)
	e.GET("/api/v1/calls/:pk/trace", s.handleCallTrace)
	// gzip is per-route: /tree wants it (02 §2.5.5), while /trace serves raw
	// bytes with Range support, which the middleware would break.
	e.GET("/api/v1/calls/:pk/tree", s.handleCallTree, middleware.Gzip())
	if s.ui != nil {
		// The embedded single-page app under its build-time base (07 §6). The
		// specific /api/v1 routes above win over the catch-all by echo's static-
		// over-wildcard priority; /metrics and the health routes never reach echo
		// (they are served by the outer mux/gate ahead of it).
		if s.uiPrefix == "" {
			e.GET("/", s.handleUI, middleware.Gzip())
			e.GET("/*", s.handleUI, middleware.Gzip())
		} else {
			// A sub-path build leaves the origin root unserved, so send it to
			// the app: opening the service host lands on the UI instead of a
			// 404. The redirect is temporary because the base is a build-time
			// switch — a root rebuild must not fight a cached 301.
			e.GET("/", func(c echo.Context) error {
				return c.Redirect(http.StatusFound, s.uiPrefix+"/")
			})
			e.GET(s.uiPrefix, s.handleUI, middleware.Gzip())
			e.GET(s.uiPrefix+"/*", s.handleUI, middleware.Gzip())
		}
	}
}

// handleConfig serves GET /api/v1/config: static, deployment-specific values
// the UI cannot derive on its own.
func (s *Service) handleConfig(c echo.Context) error {
	return c.JSON(http.StatusOK, configResponse{
		DumpsCollectorURL:  s.cfg.DumpsCollectorURL,
		FilterCapabilities: model.Capabilities(),
	})
}

func sendProblem(c echo.Context, p problem) error {
	return httpproblem.Send(c, p)
}

// badRequest rejects a request the caller has to fix. A cursor failure is not
// one of those — it goes through cursorRejected.
func badRequest(c echo.Context, detail string) error {
	return sendProblem(c, problem{Problem: httpproblem.New(http.StatusBadRequest,
		httpproblem.CodeInvalidRequest, "invalid request", detail)})
}

// cursorRejected refuses a pagination cursor: expired, malformed, or
// contradicted by re-sent filters (02 §2.3.1). It carries its own code because
// the client's answer — restart from page 1 — is the opposite of what it does
// with a badRequest, and a paging client must not have to read the detail
// prose to tell the two apart.
func cursorRejected(c echo.Context, detail string) error {
	return sendProblem(c, problem{Problem: httpproblem.New(http.StatusBadRequest,
		httpproblem.CodeCursorRejected, "invalid request", detail)})
}

func gatewayTimeout(c echo.Context, reasons []string) error {
	return sendProblem(c, problem{Problem: httpproblem.New(http.StatusGatewayTimeout,
		httpproblem.CodeNoSourceAvailable, "no data source available", strings.Join(reasons, "; "))})
}

// handleCalls serves GET /api/v1/calls (02 §2.3): validate or thaw the frozen
// query, run the wide-query guard on page 1, then fan out to both tiers —
// every hot replica plus the cutoff-bounded cold LIST — and k-way merge with
// cold-preferred PK dedup (§2.3.1, §4.3, §6). The fan-out is re-issued whole
// on every page; the cursor carries the only cross-page state.
func (s *Service) handleCalls(c echo.Context) error {
	ctx := c.Request().Context()
	params := c.QueryParams()
	allBands := model.DurationBands(s.cold.DurationThresholds)

	var q model.CallsQuery
	var after *model.Position
	var bands []model.DurationBand
	frozen := params
	band := ""
	firstPage := !params.Has("cursor")
	if firstPage {
		parsed, errDetail := model.ParseCallsQuery(params)
		if errDetail != "" {
			return badRequest(c, errDetail)
		}
		q = parsed
		// A first page reads the first band of the walk (02 §2.3.5).
		bands = q.Order.WalkBands(allBands)
		band = bands[0].Class
	} else {
		tok, err := decodeCursor(params.Get("cursor"), s.cfg.CursorTTL)
		if err != nil {
			return cursorRejected(c, err.Error())
		}
		// Pages 2..N run against the frozen query; re-sent filters must match
		// it or the request is rejected (02 §2.3.1).
		frozenQ, err := tok.query()
		if err != nil {
			return cursorRejected(c, err.Error())
		}
		if detail := frozenQueryMismatch(tok.Params, frozenQ, params); detail != "" {
			return cursorRejected(c, detail)
		}
		bands = frozenQ.Order.WalkBands(allBands)
		if frozenQ.Order.ByDuration() {
			if band, err = cursorBand(frozenQ.Order, bands, tok); err != nil {
				return cursorRejected(c, err.Error())
			}
		}
		q, frozen = frozenQ, tok.Params
		after = &tok.Pos
	}

	// The span guard runs on every page, not just page 1: the cursor carries
	// the frozen query unauthenticated (HMAC deferred, 02 §2.3.1), so a forged
	// cursor could otherwise smuggle a wide query past both guard layers
	// (PR 708 review #2). A cursor we minted for a query that already passed on
	// page 1 re-passes here — the window is identical — so honest pagination is
	// unaffected; guardCost runs after cold discovery below on the same footing.
	if rej := s.guardSpan(q, s.cfg.WideRangeLimit, s.cfg.PodsRangeLimit); rej != nil {
		return s.sendGuardRejection(c, rej)
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

	// The one lease that owns everything this request retains — the cold
	// accumulator, the hot runs, and the final page — until the response is
	// written (02 §7.5). Sources only transfer into it.
	page := s.budget.NewLease()
	defer page.Release()

	// Cold tier, bounded by the dynamic cutoff (02 §4.3). A window that ends
	// inside every replica's hot coverage skips the LIST entirely.
	var runs [][]model.CallRow
	var stops []model.Position
	var coldDiscovery *cold.Discovery
	more := false
	if coldTo := tier.coldToMs(q, s.cfg.OverlapMargin.Milliseconds()); coldTo > q.FromMs {
		coldQ := q
		coldQ.ToMs = coldTo
		scan := s.coldCalls
		if q.Order.ByDuration() {
			scan = s.coldCallsByDuration(bands, band)
		}
		res, rej, err := scan(ctx, coldQ, after, limit, page)
		coldDiscovery = res.discovery
		if rej != nil {
			return s.sendGuardRejection(c, rej)
		}
		if err != nil {
			if cold.IsBudgetDenial(err) {
				return s.sendBudgetRejection(c, budgetEndpointCalls, err, &q, coldDiscovery)
			}
			return err
		}
		partial = append(partial, res.partialReasons...)
		if res.allFailed {
			failed++
		} else {
			succeeded++
		}
		if len(res.rows) > 0 {
			runs = append(runs, res.rows)
		}
		if res.stop != nil {
			stops = append(stops, *res.stop)
		}
		more = more || res.more
	}

	// Hot tier: one sorted run per healthy replica (02 §3, §7.2). A replica
	// that filled its page may hold more rows past it, so it keeps next_cursor
	// alive; the §2.3.1 termination tolerates the resulting empty last page.
	match, err := q.Matcher()
	if err != nil {
		return badRequest(c, err.Error())
	}
	hotRuns, hotFullAt, hotOK, hotFail, hotReasons, budgetDenied := s.hotCalls(ctx, tier, q, match, after, limit, page)
	if budgetDenied != nil {
		// A budget denial fails the whole page atomically (02 §7.5): serving
		// the other sources' rows would advance the cursor past the rows the
		// denied source never produced.
		return s.sendBudgetRejection(c, budgetEndpointCalls, budgetDenied, &q, coldDiscovery)
	}
	succeeded += hotOK
	failed += hotFail
	partial = append(partial, hotReasons...)
	stops = append(stops, hotFullAt...)
	runs = append(runs, hotRuns...)

	// §8: 504 only when every attempted source failed — no data at all.
	if succeeded == 0 && failed > 0 {
		return gatewayTimeout(c, partial)
	}

	rows, mergeMore := model.MergeRunsBy(q.Order, runs, limit)
	more = more || mergeMore || len(stops) > 0
	var cursorAt *model.Position
	if len(rows) > 0 {
		last := rows[len(rows)-1].Position()
		cursorAt = &last
	}
	if stop, ok := earliestPosition(q.Order, stops); ok {
		// Rows past a stop are not known to be complete: a full replica holds
		// unseen rows past its last one, and a duration band the guard did
		// not let the page open holds unseen rows below its upper bound. Up
		// to the stop every source has answered in full, so a short page may
		// resume from the stop.
		rows = slices.DeleteFunc(rows, func(r model.CallRow) bool { return q.Order.Past(r.Position(), stop) })
		if len(rows) < limit {
			cursorAt = &stop
		}
	}

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
	if more && cursorAt != nil {
		nextBand := ""
		if q.Order.ByDuration() {
			nextBand = model.BandFor(q.Order, bands, *cursorAt)
		}
		cur := encodeCursor(frozen, *cursorAt, nextBand)
		resp.NextCursor = &cur
	}
	return c.JSON(http.StatusOK, resp)
}

// earliestPosition returns the position that sorts first in the order.
func earliestPosition(order model.Order, ps []model.Position) (model.Position, bool) {
	if len(ps) == 0 {
		return model.Position{}, false
	}
	first := ps[0]
	for _, p := range ps[1:] {
		if order.Before(p, first) {
			first = p
		}
	}
	return first, true
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
	if rej := guardPodsSpan(fromMs, toMs, s.cfg.PodsRangeLimit); rej != nil {
		return s.sendGuardRejection(c, rej)
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

// The endpoint label of the budget-denial counter (02 §7.5).
const (
	budgetEndpointCalls = "calls"
	budgetEndpointPoint = "point"
	budgetEndpointStats = "stats"
)

// sendBudgetRejection answers a read-budget denial (02 §7.5): an atomic 503
// with Retry-After, in the guard's problem dialect — the narrowing filters
// that would shrink the request's appetite and, when discovery already ran,
// the scan estimate. The detail separates transient contention from a
// structural misfit.
func (s *Service) sendBudgetRejection(c echo.Context, endpoint string, cause error, q *model.CallsQuery, disc *cold.Discovery) error {
	reason := "exhausted"
	detail := "the process-wide read memory budget is exhausted by concurrent requests; retry later or narrow the query"
	if errors.Is(cause, budget.ErrNeverFits) {
		reason = "never_fits"
		detail = "a single read of this request exceeds the whole read memory budget (PROFILER_READ_MEMORY_BUDGET); the budget is undersized for this data"
	}
	s.metrics.countBudgetDenial(endpoint, reason)

	retry := int(math.Ceil(s.cfg.ReadBudgetWait.Seconds()))
	if retry < 1 {
		retry = 1
	}
	c.Response().Header().Set("Retry-After", strconv.Itoa(retry))

	p := problem{Problem: httpproblem.New(http.StatusServiceUnavailable,
		httpproblem.CodeReadBudgetExhausted, "read memory budget exhausted", detail)}
	if q != nil {
		p.SuggestedFilters = suggestedFilters(*q)
	}
	if disc != nil {
		files, totalBytes, byClass := scanEstimate(disc.Files)
		p.EstimatedFiles = &files
		p.EstimatedBytes = &totalBytes
		p.ByClass = byClass
	}
	return sendProblem(c, p)
}

func (s *Service) sendGuardRejection(c echo.Context, rej *guardRejection) error {
	s.metrics.countGuardRejection(rej.Layer)
	p := problem{
		Problem: httpproblem.New(http.StatusBadRequest, httpproblem.CodeQueryTooWide,
			"query too wide", rej.Detail),
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
// query (02 §2.3.1). A re-sent parameter replaces its frozen value and an
// omitted one keeps it; the result must hold the same window and the same
// set of conditions, compared in canonical form, and the same order
// (02 §2.3.5).
func frozenQueryMismatch(frozen url.Values, frozenQ model.CallsQuery, params url.Values) string {
	merged := url.Values{}
	for name, vs := range frozen {
		merged[name] = vs
	}
	for name, vs := range frozenParams(params) {
		merged[name] = vs
	}
	resent, errDetail := model.ParseCallsQuery(merged)
	if errDetail != "" {
		return errDetail
	}
	if resent.FromMs != frozenQ.FromMs || resent.ToMs != frozenQ.ToMs || resent.Order != frozenQ.Order ||
		!slices.Equal(model.CanonicalConditions(resent.Filters), model.CanonicalConditions(frozenQ.Filters)) {
		return "re-sent filters do not match the query frozen in the cursor; restart from page 1"
	}
	return ""
}
