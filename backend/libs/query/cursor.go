package query

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/clock"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// cursorVersion guards the token format; a decoder bump invalidates every
// outstanding cursor, which the client handles as a page-1 restart.
const cursorVersion = 1

// cursorToken is the opaque /calls pagination cursor (02 §2.3.1): the frozen
// query, the last-emitted position, and the issue time for the TTL. The
// wide-query guard verdict rides implicitly — a cursor exists only for a
// query that passed the guard on page 1 (§2.3.2). It is URL-safe base64 of
// JSON; an HMAC signature is deferred per the contract.
type cursorToken struct {
	V        int              `json:"v"`
	Query    model.CallsQuery `json:"q"`
	Pos      model.Position   `json:"pos"`
	IssuedAt int64            `json:"iat"` // Unix ms
}

func encodeCursor(q model.CallsQuery, pos model.Position) string {
	body, err := json.Marshal(cursorToken{
		V:        cursorVersion,
		Query:    q,
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
