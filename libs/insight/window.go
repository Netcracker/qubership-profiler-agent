package insight

import (
	"strconv"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/clock"
	"github.com/pkg/errors"
)

// DefaultSince is the window an operation reads when the caller names none.
const DefaultSince = time.Hour

// Window is the time range of an operation: a relative Since, or an absolute
// From and To. The zero Window means the last DefaultSince.
type Window struct {
	Since time.Duration
	From  time.Time
	To    time.Time // zero means now
}

// ResolvedWindow is the absolute window an operation ran over. Every result
// echoes it, so the caller sees which "last hour" was meant.
type ResolvedWindow struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// ParseWindow reads the window inputs of the CLI and MCP tools: since is a
// duration such as 30m, 6h, or 2d; from and to are RFC 3339 times or Unix
// milliseconds. since and from exclude each other; all empty means the last
// DefaultSince.
func ParseWindow(since, from, to string) (Window, error) {
	var w Window
	if since != "" && from != "" {
		return w, errors.New("give either since or from, not both")
	}
	if since != "" {
		d, err := parseSince(since)
		if err != nil {
			return w, err
		}
		w.Since = d
	}
	var err error
	if from != "" {
		if w.From, err = parseTime(from); err != nil {
			return w, errors.Wrap(err, "from")
		}
	}
	if to != "" {
		if w.To, err = parseTime(to); err != nil {
			return w, errors.Wrap(err, "to")
		}
	}
	return w, nil
}

// ParseDuration reads a positive duration such as 30m, 6h, or 2d, as the
// window inputs take it.
func ParseDuration(text string) (time.Duration, error) {
	return parseSince(text)
}

// parseSince accepts a Go duration plus the unit d for days.
func parseSince(text string) (time.Duration, error) {
	text = strings.TrimSpace(text)
	var d time.Duration
	var err error
	if days, ok := strings.CutSuffix(text, "d"); ok {
		var n float64
		n, err = strconv.ParseFloat(days, 64)
		d = time.Duration(n * float64(24*time.Hour))
	} else {
		d, err = time.ParseDuration(text)
	}
	if err != nil || d <= 0 {
		return 0, errors.Errorf("since %q is not a positive duration such as 30m, 6h, or 2d", text)
	}
	return d, nil
}

func parseTime(text string) (time.Time, error) {
	text = strings.TrimSpace(text)
	if ms, err := strconv.ParseInt(text, 10, 64); err == nil {
		return time.UnixMilli(ms).UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, text)
	if err != nil {
		return time.Time{}, errors.Errorf("%q is not an RFC 3339 time or Unix milliseconds", text)
	}
	return t.UTC(), nil
}

// Resolve turns the window into absolute bounds at the current time.
func (w Window) Resolve() (ResolvedWindow, error) {
	now := clock.Now().UTC()
	to := w.To
	if to.IsZero() {
		to = now
	}
	var from time.Time
	switch {
	case !w.From.IsZero():
		from = w.From
	case w.Since > 0:
		from = to.Add(-w.Since)
	default:
		from = to.Add(-DefaultSince)
	}
	if !to.After(from) {
		return ResolvedWindow{}, errors.Errorf("the window ends (%s) before it starts (%s)",
			to.Format(time.RFC3339), from.Format(time.RFC3339))
	}
	// The API counts in whole milliseconds; trimming here keeps the echoed
	// window equal to the one the server ran.
	return ResolvedWindow{From: from.Truncate(time.Millisecond), To: to.Truncate(time.Millisecond)}, nil
}

func (r ResolvedWindow) values() map[string][]string {
	return map[string][]string{
		"from": {strconv.FormatInt(r.From.UnixMilli(), 10)},
		"to":   {strconv.FormatInt(r.To.UnixMilli(), 10)},
	}
}
