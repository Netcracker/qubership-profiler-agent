// Package hotread serves the collector's internal hot-read API
// (02-read-contract.md §3): /internal/v1 answers from the replica's own hot
// store — the SQLite call index, the in-RAM dictionaries, and the trace
// segments — and never from S3. Aggregation across replicas and tiers is the
// query service's job (libs/query); this API only has to hand it rows in the
// tier-shared (ts_ms DESC, pk ASC) order so the merge and the keyset cursor
// cannot diverge between tiers (§2.3.1).
package hotread

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/clock"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/labstack/echo/v4"
)

// The §2.3 page-size bounds; the fan-out always sends an explicit limit, the
// defaults only back a manual curl.
const (
	defaultLimit = 100
	maxLimit     = 1000
)

// API is the /internal/v1 surface over one replica's hot store.
type API struct {
	store *hotstore.Store
	echo  *echo.Echo
}

// New wires the routes; nothing binds until Run (or the caller serves
// Handler itself).
func New(store *hotstore.Store) *API {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	a := &API{store: store, echo: e}
	e.GET("/internal/v1/calls", a.handleCalls)
	e.GET("/internal/v1/calls/:pk", a.handleCall)
	e.GET("/internal/v1/calls/:pk/trace", a.handleTrace)
	e.GET("/internal/v1/pods", a.handlePods)
	e.GET("/internal/v1/pods/:podRestart/dictionary", a.handleDictionary)
	e.GET("/internal/v1/pods/:podRestart/suspend", a.handleSuspend)
	e.GET("/internal/v1/pods/:podRestart/values", a.handleValues)
	e.GET("/internal/v1/health/hot-window", a.handleHotWindow)
	return a
}

// Handler exposes the HTTP surface for tests and embedding.
func (a *API) Handler() http.Handler { return a.echo }

// Run serves /internal/v1 until ctx is cancelled.
func (a *API) Run(ctx context.Context, addr string) error {
	go func() {
		<-ctx.Done()
		_ = a.echo.Shutdown(context.Background())
	}()
	err := a.echo.Start(addr)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// problem is the RFC 7807 error body (02 §8); the internal API mirrors the
// external shapes.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

func sendProblem(c echo.Context, status int, title, detail string) error {
	c.Response().Header().Set(echo.HeaderContentType, "application/problem+json")
	c.Response().WriteHeader(status)
	return json.NewEncoder(c.Response()).Encode(problem{
		Type: "about:blank", Title: title, Status: status, Detail: detail,
	})
}

func badRequest(c echo.Context, detail string) error {
	return sendProblem(c, http.StatusBadRequest, "invalid request", detail)
}

func notFound(c echo.Context, detail string) error {
	return sendProblem(c, http.StatusNotFound, "not found", detail)
}

// callsResponse mirrors the external /calls envelope (02 §3: same JSON
// shapes). One replica is one complete source: next_cursor stays null (the
// global cursor is minted by query) and partial stays false (a failing
// replica fails the request; §7.4 partiality is the fan-out's concern).
type callsResponse struct {
	Calls          []model.CallJSON `json:"calls"`
	NextCursor     *string          `json:"next_cursor"`
	Partial        bool             `json:"partial"`
	PartialReasons []string         `json:"partial_reasons"`
}

// handleCalls serves GET /internal/v1/calls: the §2.3 filter params plus the
// keyset position (after_ts_ms / after_pk) the fan-out seeks past on pages
// 2..N (§2.3.1). Rows come only from this replica's call index.
func (a *API) handleCalls(c echo.Context) error {
	params := c.QueryParams()
	q, errDetail := model.ParseCallsQuery(params)
	if errDetail != "" {
		return badRequest(c, errDetail)
	}
	after, errDetail := parseAfter(params.Get("after_ts_ms"), params.Get("after_pk"))
	if errDetail != "" {
		return badRequest(c, errDetail)
	}
	limit := defaultLimit
	if raw := params.Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 || v > maxLimit {
			return badRequest(c, "limit must be in 1.."+strconv.Itoa(maxLimit))
		}
		limit = v
	}

	rows, err := a.queryCalls(q, after, limit)
	if err != nil {
		return err
	}
	resp := callsResponse{Calls: make([]model.CallJSON, 0, len(rows)), PartialReasons: []string{}}
	for _, row := range rows {
		resp.Calls = append(resp.Calls, row.JSON())
	}
	return c.JSON(http.StatusOK, resp)
}

func parseAfter(tsRaw, pkRaw string) (*model.Position, string) {
	if tsRaw == "" && pkRaw == "" {
		return nil, ""
	}
	if tsRaw == "" || pkRaw == "" {
		return nil, "after_ts_ms and after_pk must be sent together"
	}
	tsMs, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return nil, "after_ts_ms must be Unix ms"
	}
	pk, err := model.ParsePKPath(pkRaw)
	if err != nil {
		return nil, err.Error()
	}
	return &model.Position{TsMs: tsMs, PK: pk}, ""
}

// queryCalls walks the call partitions newest-first. Partitions are disjoint
// ts ranges, so their per-partition sorted runs concatenate into the global
// (ts_ms DESC, pk ASC) order and the walk stops at limit. The SQL-pushable
// filters and the page bound run inside the partition (№15) — a request no
// longer loads a whole bucket; the pages descend by ts until the limit fills
// or the bucket exhausts, so the Go-side filters (method, the after-PK seek)
// can reject rows without losing deeper ones. The PK order is applied in Go
// with the shared model comparator: the partitions key rows by the scalar
// pod_restart string, whose byte order diverges from the component-wise
// §2.3.1 collation (a pod name that prefixes another compares through the
// '/' separator, and restart_time_ms would compare as text).
func (a *API) queryCalls(q model.CallsQuery, after *model.Position, limit int) ([]model.CallRow, error) {
	cfg := a.store.Config()
	buckets, err := a.store.Buckets()
	if err != nil {
		return nil, err
	}
	toMs := q.ToMs
	if after != nil && after.TsMs+1 < toMs {
		toMs = after.TsMs + 1 // the seek admits no row above the cursor ts
	}

	var out []model.CallRow
	for i := len(buckets) - 1; i >= 0 && len(out) < limit; i-- {
		bucket := buckets[i]
		start := cfg.BucketStartMs(bucket)
		if start >= toMs || start+cfg.TimeBucket.Milliseconds() <= q.FromMs {
			continue
		}
		var rows []model.CallRow
		pageToMs := toMs
		for len(out)+len(rows) < limit {
			want := limit - len(out) - len(rows)
			idxRows, err := a.store.CallsPage(bucket, q, q.FromMs, pageToMs, want)
			if err != nil {
				return nil, err
			}
			if len(idxRows) == 0 {
				break
			}
			for _, idx := range idxRows {
				row, err := a.toCallRow(idx)
				if err != nil {
					return nil, err
				}
				if !q.Match(row) {
					continue
				}
				if after != nil && !row.Position().After(*after) {
					continue
				}
				rows = append(rows, row)
			}
			// Rows come newest-first with complete tie groups: fewer rows than
			// asked means the window is exhausted; otherwise the next page
			// starts strictly below the last ts.
			pageToMs = idxRows[len(idxRows)-1].TsMs
			if len(idxRows) < want || pageToMs <= q.FromMs {
				break
			}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Position().Before(rows[j].Position()) })
		out = append(out, rows...)
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// toCallRow maps an index row to the merged row shape: the method comes from
// the row's own method_text when the fast-path WAL purge materialized it
// (03 §3.9 — the pod-restart's dictionary is gone by then), otherwise from a
// targeted DictWord lookup against the pod-restart's dictionary (№15 — no
// full-map copy per request; a missing word keeps the "#<id>" placeholder, as
// the write path does); params decode from the indexed JSON. error_flag,
// retention_class, and suspend_ms are the provisional index values — the seal
// re-derives them for the cold copy, which is why the §6.3 dedup prefers cold.
func (a *API) toCallRow(idx hotstore.CallIndexRow) (model.CallRow, error) {
	key, err := hotstore.ParsePodRestartKey(idx.PodRestart)
	if err != nil {
		return model.CallRow{}, err
	}
	method := idx.MethodText
	if method == "" {
		method = fmt.Sprintf("#%d", idx.MethodId)
		if pr, live := a.store.PodRestart(key); live {
			if w, ok := pr.DictWord(idx.MethodId); ok {
				method = w
			}
		}
	}
	row := model.CallRow{
		PK: model.PK{
			PodNamespace:   key.Namespace,
			PodService:     key.Service,
			PodName:        key.PodName,
			RestartTimeMs:  key.RestartTimeMs,
			TraceFileIndex: int32(idx.TraceFileIndex),
			BufferOffset:   int32(idx.BufferOffset),
			RecordIndex:    int32(idx.RecordIndex),
		},
		TsMs:           idx.TsMs,
		DurationMs:     int32(idx.DurationMs),
		Method:         method,
		ThreadName:     idx.ThreadName,
		CpuTimeMs:      idx.CpuTimeMs,
		WaitTimeMs:     idx.WaitTimeMs,
		MemoryUsed:     idx.MemoryUsed,
		QueueWaitMs:    int32(idx.QueueWaitMs),
		SuspendMs:      int32(idx.SuspendMs),
		ChildCalls:     int32(idx.ChildCalls),
		Transactions:   int32(idx.Transactions),
		LogsGenerated:  idx.LogsGenerated,
		LogsWritten:    idx.LogsWritten,
		FileRead:       idx.FileRead,
		FileWritten:    idx.FileWritten,
		NetRead:        idx.NetRead,
		NetWritten:     idx.NetWritten,
		ErrorFlag:      idx.ErrorFlag,
		RetentionClass: idx.RetentionClass,
		Tier:           model.TierHot,
	}
	if idx.ParamsJson != "" {
		if err := json.Unmarshal([]byte(idx.ParamsJson), &row.Params); err != nil {
			return model.CallRow{}, fmt.Errorf("decode params of %s: %w", idx.PodRestart, err)
		}
	}
	return row, nil
}

// handleCall serves GET /internal/v1/calls/{pk}: a single-row fetch from this
// replica (02 §3).
func (a *API) handleCall(c echo.Context) error {
	pk, err := model.ParsePKPath(c.Param("pk"))
	if err != nil {
		return badRequest(c, err.Error())
	}
	key := podRestartKey(pk)
	idx, ok, err := a.store.FindCall(key, int(pk.TraceFileIndex), int(pk.BufferOffset), int(pk.RecordIndex))
	if err != nil {
		return err
	}
	if !ok {
		return notFound(c, "this replica holds no call "+pk.PathString())
	}
	row, err := a.toCallRow(idx)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, row.JSON())
}

// handleTrace serves GET /internal/v1/calls/{pk}/trace: the per-call blob
// assembled from the hot segments by the seal pass's chunk walk (01 §4.3,
// §4.5), with the §2.4 caching headers. 404 when the blob cannot be
// assembled — absent is a state, not an error.
func (a *API) handleTrace(c echo.Context) error {
	pk, err := model.ParsePKPath(c.Param("pk"))
	if err != nil {
		return badRequest(c, err.Error())
	}
	blob, err := a.store.AssembleTraceBlob(c.Request().Context(), podRestartKey(pk),
		int(pk.TraceFileIndex), int64(pk.BufferOffset), int(pk.RecordIndex))
	if errors.Is(err, hotstore.ErrBlobUnavailable) {
		return notFound(c, err.Error())
	}
	if err != nil {
		return err
	}
	h := c.Response().Header()
	h.Set(echo.HeaderContentType, echo.MIMEOctetStream)
	h.Set("ETag", pkETag(pk))
	h.Set("Cache-Control", "public, max-age=31536000, immutable")
	// ServeContent covers Range and If-None-Match (02 §2.4).
	http.ServeContent(c.Response(), c.Request(), "", time.Time{}, bytes.NewReader(blob))
	return nil
}

// pkETag is the §2.4 stable PK hash: the blob is immutable per PK.
func pkETag(pk model.PK) string {
	sum := sha256.Sum256([]byte(pk.PathString()))
	return `"` + hex.EncodeToString(sum[:8]) + `"`
}

func podRestartKey(pk model.PK) hotstore.PodRestartKey {
	return hotstore.PodRestartKey{
		Namespace: pk.PodNamespace, Service: pk.PodService,
		PodName: pk.PodName, RestartTimeMs: pk.RestartTimeMs,
	}
}

// podsResponse mirrors the external /pods envelope over the §2.7 entry shape.
type podsResponse struct {
	Pods           []model.PodEntry `json:"pods"`
	Partial        bool             `json:"partial"`
	PartialReasons []string         `json:"partial_reasons"`
}

// handlePods serves GET /internal/v1/pods: the pod-restarts this replica
// holds indexed calls for within [from, to), with their data bounds — the
// hot half of the §2.7 union.
func (a *API) handlePods(c echo.Context) error {
	fromMs, toMs, errDetail := model.ParseWindow(c.QueryParams())
	if errDetail != "" {
		return badRequest(c, errDetail)
	}
	windows, err := a.store.PodWindows()
	if err != nil {
		return err
	}
	resp := podsResponse{Pods: []model.PodEntry{}, PartialReasons: []string{}}
	for podRestart, w := range windows {
		if w[0] >= toMs || w[1] < fromMs {
			continue
		}
		key, err := hotstore.ParsePodRestartKey(podRestart)
		if err != nil {
			return err
		}
		resp.Pods = append(resp.Pods, model.PodEntry{
			PodTuple: model.PodTuple{
				Namespace: key.Namespace, Service: key.Service,
				Pod: key.PodName, RestartTimeMs: key.RestartTimeMs,
			},
			TimeMinMs: w[0],
			TimeMaxMs: w[1],
		})
	}
	resp.Pods = model.UnionPods(resp.Pods) // sole source; used for the stable sort
	return c.JSON(http.StatusOK, resp)
}

// dictionarySnapshot is the §2.6 response shape; both arrays carry the full
// word list because the wire dictionary is one id space (01 §3.6).
type dictionarySnapshot struct {
	Version int      `json:"version"`
	Methods []string `json:"methods"`
	Params  []string `json:"params"`
}

// handleDictionary serves GET /internal/v1/pods/{pod-restart}/dictionary for
// pod-restarts hosted by this replica (02 §2.6, §3). The ETag is
// (pod-restart, version); a live dictionary only grows, so If-None-Match
// revalidation answers 304 until it does.
func (a *API) handleDictionary(c echo.Context) error {
	tuple, err := model.ParsePodRestartPath(c.Param("podRestart"))
	if err != nil {
		return badRequest(c, err.Error())
	}
	key := hotstore.PodRestartKey{
		Namespace: tuple.Namespace, Service: tuple.Service,
		PodName: tuple.Pod, RestartTimeMs: tuple.RestartTimeMs,
	}
	pr, ok := a.store.PodRestart(key)
	if !ok {
		return notFound(c, "this replica hosts no pod-restart "+c.Param("podRestart"))
	}
	words := pr.DictionaryWords()
	etag := fmt.Sprintf(`"%s:%d"`, c.Param("podRestart"), len(words))
	c.Response().Header().Set("ETag", etag)
	if c.Request().Header.Get("If-None-Match") == etag {
		return c.NoContent(http.StatusNotModified)
	}
	return c.JSON(http.StatusOK, dictionarySnapshot{Version: len(words), Methods: words, Params: words})
}

// suspendTimeline is the suspend endpoint's body: the pod-restart's
// stop-the-world pauses in the same (end, duration) event shape the sealed
// rows inline as suspend_json, so a consumer parses one format on either
// tier.
type suspendTimeline struct {
	Events []suspendTimelineEvent `json:"events"`
}

type suspendTimelineEvent struct {
	// EndMs is the pause end (the agent timestamps a delay after detecting it);
	// the pause spans [EndMs − DurationMs, EndMs] — the same event shape as
	// the sealed rows' suspend_json column, so a consumer parses one format
	// on either tier (№4).
	EndMs      int64 `json:"end_ms"`
	DurationMs int   `json:"duration_ms"`
}

// handleSuspend serves GET /internal/v1/pods/{pod-restart}/suspend: the
// global suspension timeline the /tree rendering intersects node work
// intervals with (08-ui-backend-requirements.md R7). The timeline comes from
// the replica's RAM mirror of suspend.wal, which recovery reloads, so live
// and recovered pod-restarts both answer.
func (a *API) handleSuspend(c echo.Context) error {
	tuple, err := model.ParsePodRestartPath(c.Param("podRestart"))
	if err != nil {
		return badRequest(c, err.Error())
	}
	key := hotstore.PodRestartKey{
		Namespace: tuple.Namespace, Service: tuple.Service,
		PodName: tuple.Pod, RestartTimeMs: tuple.RestartTimeMs,
	}
	pr, ok := a.store.PodRestart(key)
	if !ok {
		return notFound(c, "this replica hosts no pod-restart "+c.Param("podRestart"))
	}
	pauses := pr.SuspendPauses()
	body := suspendTimeline{Events: make([]suspendTimelineEvent, 0, len(pauses))}
	for _, p := range pauses {
		body.Events = append(body.Events, suspendTimelineEvent{EndMs: p.TimeMs, DurationMs: p.DurationMs})
	}
	return c.JSON(http.StatusOK, body)
}

// valuesResponse maps the resolved big-parameter references
// ("<stream>:<seq>:<offset>" → value). A reference that did not resolve is
// absent, and the caller marks it unresolved in the tree it renders.
type valuesResponse struct {
	Values map[string]string `json:"values"`
}

// handleValues serves GET /internal/v1/pods/{pod-restart}/values?ref=...: the
// big-parameter values of this replica's sql / xml value segments (01 §4.4).
// The query service's /tree path fetches a call's references in one batch;
// the value streams stay internal — the external API never exposes them
// (02 §2.5).
func (a *API) handleValues(c echo.Context) error {
	tuple, err := model.ParsePodRestartPath(c.Param("podRestart"))
	if err != nil {
		return badRequest(c, err.Error())
	}
	rawRefs := c.QueryParams()["ref"]
	if len(rawRefs) == 0 {
		return badRequest(c, "at least one ref=<stream>:<seq>:<offset> is required")
	}
	refs := make([]hotstore.ValueRef, 0, len(rawRefs))
	for _, raw := range rawRefs {
		ref, err := hotstore.ParseValueRef(raw)
		if err != nil {
			return badRequest(c, err.Error())
		}
		refs = append(refs, ref)
	}
	key := hotstore.PodRestartKey{
		Namespace: tuple.Namespace, Service: tuple.Service,
		PodName: tuple.Pod, RestartTimeMs: tuple.RestartTimeMs,
	}
	if _, ok := a.store.PodRestart(key); !ok {
		return notFound(c, "this replica hosts no pod-restart "+c.Param("podRestart"))
	}
	values, err := a.store.BigValues(c.Request().Context(), key, refs)
	if err != nil {
		return err
	}
	resp := valuesResponse{Values: make(map[string]string, len(values))}
	for ref, value := range values {
		resp.Values[ref.String()] = value
	}
	return c.JSON(http.StatusOK, resp)
}

// hotWindow is the §3 health report the query service derives the dynamic
// cold cutoff from (§4.3).
type hotWindow struct {
	OldestMs int64 `json:"hot_window_oldest_ms"`
	NowMs    int64 `json:"hot_window_now_ms"`
}

// handleHotWindow serves GET /internal/v1/health/hot-window. An empty index
// reports oldest = now: the window [now, now) holds nothing, so the cold
// tier keeps covering the whole query range.
func (a *API) handleHotWindow(c echo.Context) error {
	nowMs := clock.Now().UnixMilli()
	oldest, ok, err := a.store.HotWindowOldestMs()
	if err != nil {
		return err
	}
	if !ok {
		oldest = nowMs
	}
	return c.JSON(http.StatusOK, hotWindow{OldestMs: oldest, NowMs: nowMs})
}
