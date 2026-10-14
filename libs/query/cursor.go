package query

import (
	"encoding/base64"
	"encoding/json"
	"net/url"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/clock"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// cursorVersion guards the token format; a decoder bump invalidates every
// outstanding cursor, which the client handles as a page-1 restart. Version 2
// freezes the query parameters as sent instead of the parsed query, so the
// filter conditions of 02 §2.3.3 ride along without a second encoding.
const cursorVersion = 2

// cursorToken is the opaque /calls pagination cursor (02 §2.3.1): the frozen
// query as its page-1 parameters, the last-emitted position, and the issue
// time for the TTL. It is
// URL-safe base64 of JSON; an HMAC signature is deferred per the contract, so
// the token is client-forgeable — the wide-query guard therefore re-runs on
// the frozen query on every page rather than trusting a page-1 verdict to
// ride along (§2.3.2, PR 708 review #2).
type cursorToken struct {
	V        int            `json:"v"`
	Params   url.Values     `json:"q"`
	Pos      model.Position `json:"pos"`
	IssuedAt int64          `json:"iat"` // Unix ms
}

// queryParams are the /calls parameters that define the query: the window
// and every filter. The cursor freezes them as page 1 sent them.
var queryParams = []string{
	"from", "to", "filter", "pod", "method", "duration_min_ms", "duration_max_ms", "error_only", "retention_class",
}

// frozenParams keeps the query parameters of a request.
func frozenParams(params url.Values) url.Values {
	out := url.Values{}
	for _, name := range queryParams {
		if vs, ok := params[name]; ok {
			out[name] = vs
		}
	}
	return out
}

func encodeCursor(params url.Values, pos model.Position) string {
	body, err := json.Marshal(cursorToken{
		V:        cursorVersion,
		Params:   frozenParams(params),
		Pos:      pos,
		IssuedAt: clock.Now().UnixMilli(),
	})
	if err != nil {
		panic(err) // the token is a plain struct; marshalling cannot fail
	}
	return base64.RawURLEncoding.EncodeToString(body)
}

// decodeCursor parses and validates a client cursor. Every failure — junk,
// wrong version, or past the TTL — maps to a 400 and a client restart from
// page 1 (02 §2.3.1).
func decodeCursor(s string, ttl time.Duration) (cursorToken, error) {
	body, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursorToken{}, errors.Wrap(err, "cursor is not URL-safe base64")
	}
	var tok cursorToken
	if err := json.Unmarshal(body, &tok); err != nil {
		return cursorToken{}, errors.Wrap(err, "cursor does not decode")
	}
	if tok.V != cursorVersion {
		return cursorToken{}, errors.Errorf("cursor version %d is not supported", tok.V)
	}
	if age := clock.Now().UnixMilli() - tok.IssuedAt; age > ttl.Milliseconds() {
		return cursorToken{}, errors.Errorf("cursor expired: issued %s ago, TTL %s",
			(time.Duration(age) * time.Millisecond).Round(time.Second), ttl)
	}
	return tok, nil
}

// query parses the frozen parameters. A failure means a forged or damaged
// cursor: query minted it from parameters that parsed.
func (tok cursorToken) query() (model.CallsQuery, error) {
	q, errDetail := model.ParseCallsQuery(tok.Params)
	if errDetail != "" {
		return q, errors.New("cursor query does not parse: " + errDetail)
	}
	return q, nil
}
