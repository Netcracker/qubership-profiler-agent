package insight

import (
	"encoding/base64"
	"encoding/json"
	"slices"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// cursorVersion guards the format of the cursor Calls hands out. A cursor of
// another version restarts the listing, as one the server rejects does.
const cursorVersion = 1

// callsCursor is the NextCursor of Calls: the server's cursor plus the
// conditions the listing was authorized with.
//
// The server's cursor freezes the query of page 1, and a later page keeps
// every frozen parameter the request does not re-send (02 §2.3.1). If Calls
// authorized the conditions passed beside a cursor and sent the cursor
// alone, a cursor minted for another namespace would read that namespace.
// Calls therefore authorizes the conditions recorded here and re-sends them
// with every page, so the server rejects a cursor whose frozen query holds
// other conditions, a hand-made callsCursor included. A cursor with no
// conditions re-sends none and the server keeps the frozen ones, which is
// safe: the Authorizer then approved a read across every namespace. The
// window is not re-sent: the server keeps page 1's, so a relative window
// does not move.
//
// Order is the order of page 1, empty for the default. When the server
// rejects the cursor, Calls restarts at page 1, and a caller that continues
// with the cursor alone sends no order; the recorded one keeps a slowest-first
// listing slowest-first. A cursor from a build that did not record the order
// still decodes and restarts in the default order.
type callsCursor struct {
	V          int      `json:"v"`
	Server     string   `json:"c"`
	Conditions []string `json:"f,omitempty"`
	Order      string   `json:"o,omitempty"`
}

func encodeCallsCursor(server string, conditions []string, order string) string {
	body, err := json.Marshal(callsCursor{V: cursorVersion, Server: server, Conditions: conditions, Order: order})
	if err != nil {
		panic(err) // a struct of strings always marshals
	}
	return base64.RawURLEncoding.EncodeToString(body)
}

// decodeCallsCursor parses a cursor Calls handed out. An error means the
// cursor is damaged, comes from another version, or was not made by Calls.
func decodeCallsCursor(text string) (callsCursor, error) {
	body, err := base64.RawURLEncoding.DecodeString(text)
	if err != nil {
		return callsCursor{}, errors.Wrap(err, "cursor is not URL-safe base64")
	}
	var cur callsCursor
	if err := json.Unmarshal(body, &cur); err != nil {
		return callsCursor{}, errors.Wrap(err, "cursor does not decode")
	}
	if cur.V != cursorVersion || cur.Server == "" {
		return callsCursor{}, errors.Errorf("cursor version %d is not supported", cur.V)
	}
	if _, err := canonicalConditions(cur.Conditions); err != nil {
		return callsCursor{}, err
	}
	// Only the form is checked, as Calls checks Order: an order of the
	// contract this server does not implement is the server's to reject.
	if cur.Order != "" && !slices.Contains(CallOrders, cur.Order) {
		return callsCursor{}, errors.Errorf("cursor order %q is not supported", cur.Order)
	}
	return cur, nil
}

// canonicalConditions returns the conditions in the canonical form the
// server compares a re-sent query in: sorted, deduplicated, and respelled.
func canonicalConditions(texts []string) ([]string, error) {
	parsed := make([]model.Condition, 0, len(texts))
	for _, text := range texts {
		c, err := model.ParseCondition(text)
		if err != nil {
			return nil, errors.Wrapf(err, "filter %q", text)
		}
		parsed = append(parsed, c)
	}
	return model.CanonicalConditions(parsed), nil
}

// sameConditions reports whether two valid condition lists select the same
// calls by the server's rule: the same set in canonical form.
func sameConditions(a, b []string) bool {
	ca, errA := canonicalConditions(a)
	cb, errB := canonicalConditions(b)
	return errA == nil && errB == nil && slices.Equal(ca, cb)
}
