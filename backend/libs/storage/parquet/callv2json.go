package parquet

import (
	"encoding/json"
	"strconv"

	"github.com/pkg/errors"
)

// SuspendEvent is one stop-the-world pause of the suspend_json column: the
// pause spans [EndMs − DurationMs, EndMs] — the same (end, duration) shape
// the agent wire and the internal hot suspend endpoint carry (№4).
type SuspendEvent struct {
	EndMs      int64 `json:"end_ms"`
	DurationMs int64 `json:"duration_ms"`
}

// The encode/decode pairs below pin the JSON shapes of the self-contained
// CallV2 columns in ONE place, so the seal-pass writer and the cold /tree
// reader cannot drift apart (01-write-contract.md §5.2).

// EncodeDictWords renders the dict_words_json value; nil for an empty subset.
func EncodeDictWords(words map[int]string) (*string, error) {
	if len(words) == 0 {
		return nil, nil
	}
	keyed := make(map[string]string, len(words))
	for id, word := range words {
		keyed[strconv.Itoa(id)] = word
	}
	body, err := json.Marshal(keyed)
	if err != nil {
		return nil, errors.Wrap(err, "encode dict_words_json")
	}
	s := string(body)
	return &s, nil
}

// DecodeDictWords parses a dict_words_json column value; nil input (a NULL
// column) decodes to an empty map, so every unresolved id renders as the
// "#<id>" placeholder.
func DecodeDictWords(col *string) (map[int]string, error) {
	if col == nil {
		return nil, nil
	}
	var keyed map[string]string
	if err := json.Unmarshal([]byte(*col), &keyed); err != nil {
		return nil, errors.Wrap(err, "decode dict_words_json")
	}
	out := make(map[int]string, len(keyed))
	for key, word := range keyed {
		id, err := strconv.Atoi(key)
		if err != nil {
			return nil, errors.Wrapf(err, "decode dict_words_json id %q", key)
		}
		out[id] = word
	}
	return out, nil
}

// EncodeSuspend renders the suspend_json value; nil for an empty timeline.
func EncodeSuspend(events []SuspendEvent) (*string, error) {
	if len(events) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(events)
	if err != nil {
		return nil, errors.Wrap(err, "encode suspend_json")
	}
	s := string(body)
	return &s, nil
}

// DecodeSuspend parses a suspend_json column value; nil input (a NULL
// column) decodes to an empty timeline — zero suspension.
func DecodeSuspend(col *string) ([]SuspendEvent, error) {
	if col == nil {
		return nil, nil
	}
	var events []SuspendEvent
	if err := json.Unmarshal([]byte(*col), &events); err != nil {
		return nil, errors.Wrap(err, "decode suspend_json")
	}
	return events, nil
}
