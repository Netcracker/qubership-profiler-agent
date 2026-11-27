package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"unicode"
)

// Link is one operator-configured link-out of the /config body (02 §2.8): a
// button on the call tree page that opens the call in a tracing or logging
// system. The UI expands the placeholders of URLTemplate and URL-encodes
// each value; the backend only validates the template and passes it on.
type Link struct {
	Label       string `json:"label"`
	Kind        string `json:"kind"`
	URLTemplate string `json:"url_template"`
}

// Link kinds of 02 §2.8.
const (
	LinkKindTrace = "trace"
	LinkKindLogs  = "logs"
)

// linkPlaceholders are the names a url_template may put in braces (02 §2.8).
var linkPlaceholders = []string{"trace_id", "from_ms", "to_ms", "namespace", "service", "pod"}

// linkPlaceholderPattern matches a name of lowercase letters and underscores
// in braces, as the UI matches it. Only a name in linkPlaceholders is a
// placeholder; the UI and the server leave any other brace in the URL as
// written, so a LogQL stream selector such as {app="x"}, a regexp escape such
// as \x{a0} or \p{Greek}, and a Grafana Explore JSON state need no escaping.
var linkPlaceholderPattern = regexp.MustCompile(`\{([a-z_]+)\}`)

// linkNamePattern matches a bare name in braces, the text LinkWarnings
// compares with the placeholder names.
var linkNamePattern = regexp.MustCompile(`\{([A-Za-z_][A-Za-z0-9_.-]*)\}`)

// ParseLinks decodes the PROFILER_LINKS value, a JSON array of Link objects,
// and validates every entry. An empty value yields no links. The error names
// the offending entry by its 1-based position and label, so the operator
// can find it in a long list.
func ParseLinks(raw string) ([]Link, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	// A misspelled key (urlTemplate) must not pass as an empty template.
	// encoding/json matches keys case-insensitively, so a key is rejected
	// only when it differs from a known one by more than letter case.
	dec.DisallowUnknownFields()
	var links []Link
	if err := dec.Decode(&links); err != nil {
		return nil, fmt.Errorf("links: want a JSON array of {label, kind, url_template} objects: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("links: unexpected data after the JSON array")
	}
	if err := ValidateLinks(links); err != nil {
		return nil, err
	}
	return links, nil
}

// ValidateLinks checks every entry against 02 §2.8: a non-empty label, a
// known kind, and a template that is an absolute http(s) URL once the
// placeholders are filled in. A brace that does not form a placeholder is
// accepted as a literal character of the URL; LinkWarnings reports the ones
// that look like a misspelled placeholder.
func ValidateLinks(links []Link) error {
	for i, l := range links {
		if err := l.validate(); err != nil {
			return fmt.Errorf("links: entry %d (label %q): %w", i+1, l.Label, err)
		}
	}
	return nil
}

func (l Link) validate() error {
	if strings.TrimSpace(l.Label) == "" {
		return errors.New("label is empty")
	}
	if l.Kind != LinkKindTrace && l.Kind != LinkKindLogs {
		return fmt.Errorf("unknown kind %q, want %q or %q", l.Kind, LinkKindTrace, LinkKindLogs)
	}
	if l.URLTemplate == "" {
		return errors.New("url_template is empty")
	}
	// Fill the placeholders the way the UI does, with an encoded value, and
	// judge what is left: the URL a click would open.
	filled := linkPlaceholderPattern.ReplaceAllStringFunc(l.URLTemplate, func(m string) string {
		if slices.Contains(linkPlaceholders, m[1:len(m)-1]) {
			return "x"
		}
		return m
	})
	u, err := url.Parse(filled)
	if err != nil {
		// url.Error quotes the filled-in string; the operator wrote the template.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return fmt.Errorf("url_template %q is not a URL: %w", l.URLTemplate, err)
	}
	// The UI renders the result as an href, so the same rule as for
	// DumpsCollectorURL applies: nothing but an absolute http(s) URL.
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("url_template %q is not an absolute http(s) URL", l.URLTemplate)
	}
	return nil
}

// LinkWarnings returns one message per distinct name in braces, in each
// entry, that looks like a misspelled placeholder: {pod_name} or {POD} for
// {pod}, {traceId} or {trace.id} for {trace_id}, {from} for {from_ms}. Such
// text is valid in a URL and stays in the link as written, so query starts
// and logs the messages. A name unrelated to every placeholder, such as {a0}
// in the regexp escape \x{a0} or {Greek} in \p{Greek}, gets no message.
func LinkWarnings(links []Link) []string {
	var warnings []string
	for i, l := range links {
		var seen []string
		for _, m := range linkNamePattern.FindAllStringSubmatch(l.URLTemplate, -1) {
			name := m[1]
			if slices.Contains(linkPlaceholders, name) || slices.Contains(seen, name) {
				continue
			}
			seen = append(seen, name)
			if p, ok := resemblingPlaceholder(name); ok {
				warnings = append(warnings, fmt.Sprintf(
					"PROFILER_LINKS (Helm query.links) entry %d (label %q): url_template %q keeps %s as literal text; if it is meant to be the placeholder {%s}, write that, and if the text is meant literally, write its opening brace as %%7B",
					i+1, l.Label, l.URLTemplate, m[0], p))
			}
		}
	}
	return warnings
}

// resemblingPlaceholder returns the placeholder that name resembles once
// case, '_', '-', and '.' are ignored: name contains the placeholder, as
// podname contains pod, or name of at least three characters starts the
// placeholder, as from starts fromms.
func resemblingPlaceholder(name string) (string, bool) {
	n := normalizeLinkName(name)
	for _, p := range linkPlaceholders {
		np := normalizeLinkName(p)
		if strings.Contains(n, np) || (len(n) >= 3 && strings.HasPrefix(np, n)) {
			return p, true
		}
	}
	return "", false
}

func normalizeLinkName(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '_', '-', '.':
			return -1
		}
		return unicode.ToLower(r)
	}, s)
}
