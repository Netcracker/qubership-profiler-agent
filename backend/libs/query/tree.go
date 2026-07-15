package query

// The per-call endpoints of 02-read-contract.md: GET /api/v1/calls/{pk}/trace
// (§2.4, the raw blob) and GET /api/v1/calls/{pk}/tree (§2.5, the canonical
// MessagePack tree). Both locate the call the same way: probe the hot
// replicas first — a live call costs no S3 round-trip — and fall back to the
// cold tier, which needs the ts_ms (and ideally retention_class) hints from
// the /calls row because a bare PK carries no time (§2.2). The tree itself is
// rendered here for both tiers through libs/calltree; only the big-parameter
// values differ in origin (replica value segments vs the sealed
// big_params_json column).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

// pointHints are the §2.2 cold-location hints of a point fetch.
type pointHints struct {
	tsMs    int64
	hasTs   bool
	classes []string
}

func parsePointHints(c echo.Context) (pointHints, string) {
	var h pointHints
	if raw := c.QueryParam("ts_ms"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return h, "ts_ms must be Unix ms"
		}
		h.tsMs, h.hasTs = v, true
	}
	for _, class := range c.QueryParams()["retention_class"] {
		if !model.IsRetentionClass(class) {
			return h, "unknown retention_class: " + class
		}
		h.classes = append(h.classes, class)
	}
	return h, ""
}

// pointFetch is the outcome of the tiered call lookup.
type pointFetch struct {
	blob []byte
	// hot origin: the replica that served the blob (empty on a cold hit).
	replicaURL string
	// cold origin: the full parquet row (nil on a hot hit).
	row *storageparquet.CallV2
	// bookkeeping for the §8 all-sources-failed verdict.
	found     bool
	failed    int
	succeeded int
	reasons   []string
	truncated string // non-empty when the row exists but its blob was dropped
}

// fetchPoint runs the tiered lookup: every replica is probed for the blob,
// then the cold tier is consulted within the hint window. Cold needs ts_ms;
// without it a call that already left the hot tier is not locatable (§2.2).
func (s *Service) fetchPoint(ctx context.Context, pk model.PK, hints pointHints) pointFetch {
	var out pointFetch
	if s.discovery != nil {
		urls, err := s.discovery.Replicas(ctx)
		if err != nil {
			out.failed++
			out.reasons = append(out.reasons, fmt.Sprintf("collector discovery: %v", err))
		}
		for _, baseURL := range urls {
			blob, found, err := s.hot.Trace(ctx, baseURL, pk)
			if err != nil {
				out.failed++
				out.reasons = append(out.reasons, fmt.Sprintf("collector %s trace: %v", baseURL, err))
				continue
			}
			out.succeeded++
			if found {
				out.blob, out.replicaURL, out.found = blob, baseURL, true
				return out
			}
		}
	}

	if !hints.hasTs {
		return out
	}

	// The retention_class hint only sharpens pruning; it is optional (09 §5,
	// 02 §2.2). Seal can reclassify a call after the UI baked the old class
	// into the /tree URL — a late call.red registration bumps an error call
	// out of its clean class — so the hinted prefix would then hold nothing
	// and a bookmarked link would 404 forever (№16). Try the hinted class
	// first for the cheaper LIST, then fall back to scanning every class
	// before giving up.
	row, ok := s.discoverAndFetch(ctx, pk, hints.tsMs, hints.classes, &out)
	if !ok && len(hints.classes) > 0 && out.failed == 0 {
		row, ok = s.discoverAndFetch(ctx, pk, hints.tsMs, nil, &out)
	}
	if !ok {
		return out
	}
	out.found = true
	out.row = row
	if row.TruncatedReason != nil {
		out.truncated = *row.TruncatedReason
		return out
	}
	if row.TraceBlob != nil {
		out.blob = row.TraceBlob
	}
	return out
}

// discoverAndFetch runs one cold discovery+fetch pass over the given retention
// classes (nil = every class) for the point at tsMs. It records discovery
// failures and partial reasons on out, and reports the row plus whether the PK
// was found. A false result with out.failed unchanged means an honest cold
// miss — the caller may widen the class set and retry (№16).
func (s *Service) discoverAndFetch(ctx context.Context, pk model.PK, tsMs int64, classes []string, out *pointFetch) (*storageparquet.CallV2, bool) {
	q := model.CallsQuery{
		FromMs:           tsMs,
		ToMs:             tsMs + 1,
		RetentionClasses: classes,
	}
	discovery, err := s.cold.Discover(ctx, q)
	if err != nil {
		out.failed++
		out.reasons = append(out.reasons, fmt.Sprintf("s3 discovery: %v", err))
		return nil, false
	}
	out.reasons = append(out.reasons, discovery.PartialReasons...)
	if discovery.Prefixes > 0 && discovery.FailedPrefixes == discovery.Prefixes {
		out.failed++
		return nil, false
	}
	row, ok, err := cold.FetchCall(ctx, s.cold.Store, discovery.Files, pk)
	if err != nil {
		out.failed++
		out.reasons = append(out.reasons, fmt.Sprintf("s3 fetch: %v", err))
		return nil, false
	}
	out.succeeded++
	return row, ok
}

// pointProblem maps a miss to the §8 status: 504 only when at least one
// source failed and none produced the call; otherwise an honest 404 whose
// detail explains the §2.2 hint when it could have changed the answer.
func (s *Service) pointProblem(c echo.Context, pk model.PK, hints pointHints, fetch pointFetch) error {
	if fetch.truncated != "" {
		return sendProblem(c, problem{Title: "trace blob unavailable", Status: http.StatusNotFound,
			Detail: fmt.Sprintf("the blob of %s was dropped at seal: truncated_reason = %s", pk.PathString(), fetch.truncated)})
	}
	if fetch.succeeded == 0 && fetch.failed > 0 {
		return gatewayTimeout(c, fetch.reasons)
	}
	detail := "no tier holds call " + pk.PathString()
	if !hints.hasTs {
		detail += "; a call outside the hot window needs the ts_ms (and retention_class) hints from its /calls row (02 §2.2)"
	}
	return sendProblem(c, problem{Title: "call not found", Status: http.StatusNotFound, Detail: detail})
}

// handleCallTrace serves GET /api/v1/calls/{pk}/trace (02 §2.4): the raw
// blob, immutable per PK, with Range support. The §7.4 partial envelope does
// not apply — the blob is either present or absent.
func (s *Service) handleCallTrace(c echo.Context) error {
	pk, err := pkParam(c)
	if err != nil {
		return badRequest(c, err.Error())
	}
	hints, errDetail := parsePointHints(c)
	if errDetail != "" {
		return badRequest(c, errDetail)
	}
	fetch := s.fetchPoint(c.Request().Context(), pk, hints)
	if !fetch.found || fetch.blob == nil {
		return s.pointProblem(c, pk, hints, fetch)
	}

	h := c.Response().Header()
	h.Set(echo.HeaderContentType, echo.MIMEOctetStream)
	h.Set("ETag", pkETag(pk))
	h.Set("Cache-Control", "public, max-age=31536000, immutable")
	// ServeContent covers Range, HEAD, and If-None-Match (§2.4).
	http.ServeContent(c.Response(), c.Request(), "", time.Time{}, bytes.NewReader(fetch.blob))
	return nil
}

// handleCallTree serves GET /api/v1/calls/{pk}/tree (02 §2.5): the blob
// decoded into the int-keyed MessagePack tree, self-contained — the per-tree
// dictionary carries only the strings this tree references, and big-parameter
// values are inlined (or explicitly marked unresolved, never dropped).
func (s *Service) handleCallTree(c echo.Context) error {
	ctx := c.Request().Context()
	pk, err := pkParam(c)
	if err != nil {
		return badRequest(c, err.Error())
	}
	hints, errDetail := parsePointHints(c)
	if errDetail != "" {
		return badRequest(c, errDetail)
	}
	// §2.5.4: the header selects among the versions the server still emits.
	// Only v1 exists; an unknown request is refused rather than answered with
	// bytes the client did not ask for.
	if v := c.Request().Header.Get("Accept-Version"); v != "" && v != strconv.Itoa(calltree.Version) {
		return badRequest(c, fmt.Sprintf("unsupported Accept-Version %q: this server emits version %d (02 §2.5.4)", v, calltree.Version))
	}

	fetch := s.fetchPoint(ctx, pk, hints)
	if !fetch.found || fetch.blob == nil {
		return s.pointProblem(c, pk, hints, fetch)
	}

	tuple := model.PodTuple{
		Namespace: pk.PodNamespace, Service: pk.PodService,
		Pod: pk.PodName, RestartTimeMs: pk.RestartTimeMs,
	}
	var words []string
	var bigValue func(stream string, seq int, offset int64) (string, bool)
	var pauses []calltree.SuspendInterval
	if fetch.replicaURL != "" {
		words, err = s.hotDictionary(ctx, fetch.replicaURL, tuple)
		if err != nil {
			return gatewayTimeout(c, append(fetch.reasons, fmt.Sprintf("collector %s dictionary: %v", fetch.replicaURL, err)))
		}
		values, err := s.hotBigValues(ctx, fetch.replicaURL, tuple, fetch.blob, int(pk.RecordIndex))
		if err != nil {
			if errors.Is(err, errValuesUnavailable) {
				return gatewayTimeout(c, append(fetch.reasons, fmt.Sprintf("collector %s values: %v", fetch.replicaURL, err)))
			}
			return err
		}
		bigValue = values
		// found=false means the pod-restart left the replica between the blob
		// fetch and this call: zero suspension beats failing the whole tree.
		pauses, _, err = s.hot.Suspend(ctx, fetch.replicaURL, tuple)
		if err != nil {
			return gatewayTimeout(c, append(fetch.reasons, fmt.Sprintf("collector %s suspend: %v", fetch.replicaURL, err)))
		}
	} else {
		words, err = s.coldDictionary(ctx, tuple)
		if err != nil {
			return gatewayTimeout(c, append(fetch.reasons, fmt.Sprintf("s3 dictionary: %v", err)))
		}
		var sealed map[string]string
		if fetch.row.BigParamsJson != nil {
			if err := json.Unmarshal([]byte(*fetch.row.BigParamsJson), &sealed); err != nil {
				return errors.Wrapf(err, "decode big_params_json of %s", pk.PathString())
			}
		}
		bigValue = func(stream string, seq int, offset int64) (string, bool) {
			v, ok := sealed[fmt.Sprintf("%s:%d:%d", stream, seq, offset)]
			return v, ok
		}
		// ok=false (no snapshot object: unclean close or TTL) degrades to
		// zero suspension, the pre-R7 behaviour.
		pauses, _, err = cold.Suspend(ctx, s.cold.Store, tuple)
		if err != nil {
			return gatewayTimeout(c, append(fetch.reasons, fmt.Sprintf("s3 suspend: %v", err)))
		}
	}

	tree, err := calltree.Build(fetch.blob, int(pk.RecordIndex), calltree.Options{
		Dict: func(id int) (string, bool) {
			if id < 0 || id >= len(words) || words[id] == "" {
				return "", false
			}
			return words[id], true
		},
		BigValue: bigValue,
		Suspend:  pauses,
	})
	if err != nil {
		return errors.Wrapf(err, "decode blob of %s", pk.PathString())
	}
	body := calltree.Encode(tree)

	h := c.Response().Header()
	h.Set("ETag", pkETag(pk))
	h.Set("Cache-Control", "public, max-age=31536000, immutable")
	if c.Request().Header.Get("If-None-Match") == pkETag(pk) {
		return c.NoContent(http.StatusNotModified)
	}
	// Content-Encoding: gzip rides on the per-route middleware (§2.5.5).
	return c.Blob(http.StatusOK, "application/x-msgpack", body)
}

// errValuesUnavailable marks a big-parameter fetch that failed in transport
// (timeout, non-200, decode) rather than "the reference is genuinely absent".
// §2.5.3 forbids serving a tree whose SQL silently degraded to unresolved
// groups on such a failure, so handleCallTree answers 504 like the dictionary
// and suspend transport paths — not a 200 with corrupted R11 aggregation.
var errValuesUnavailable = errors.New("big-parameter values unavailable")

// hotBigValues resolves the blob's big-parameter references against the replica
// that served it, in one batched round-trip. References the replica cannot
// resolve are simply absent from a successful response and render as
// unresolved; a transport/decode/non-200 failure instead returns
// errValuesUnavailable, so the caller fails the tree rather than corrupting it.
func (s *Service) hotBigValues(ctx context.Context, baseURL string, tuple model.PodTuple, blob []byte, recordIndex int) (func(stream string, seq int, offset int64) (string, bool), error) {
	refs, err := calltree.CollectBigRefs(blob, recordIndex)
	if err != nil {
		return nil, errors.Wrapf(err, "scan blob for big params")
	}
	if len(refs) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(refs))
	for _, ref := range refs {
		keys = append(keys, ref.String())
	}
	values, err := s.hot.Values(ctx, baseURL, tuple, keys)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errValuesUnavailable, err)
	}
	return func(stream string, seq int, offset int64) (string, bool) {
		v, ok := values[fmt.Sprintf("%s:%d:%d", stream, seq, offset)]
		return v, ok
	}, nil
}

// pkETag is the §2.4 stable PK hash; the blob and its tree are immutable per
// PK, so the same tag serves both endpoints.
func pkETag(pk model.PK) string {
	sum := sha256.Sum256([]byte(pk.PathString()))
	return `"` + hex.EncodeToString(sum[:8]) + `"`
}
