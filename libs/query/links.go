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

var linkPlaceholderPattern = regexp.MustCompile(`\{([^{}]*)\}`)

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
// known kind, known placeholders only, and a template that is an absolute
// http(s) URL once the placeholders are filled in.
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
	for _, m := range linkPlaceholderPattern.FindAllStringSubmatch(l.URLTemplate, -1) {
		if !slices.Contains(linkPlaceholders, m[1]) {
			return fmt.Errorf("url_template %q uses unknown placeholder %s, want one of {%s}",
				l.URLTemplate, m[0], strings.Join(linkPlaceholders, "}, {"))
		}
	}
	// Fill the placeholders the way the UI does, with an encoded value, and
	// judge what is left: the URL a click would open.
	filled := linkPlaceholderPattern.ReplaceAllString(l.URLTemplate, "x")
	if strings.ContainsAny(filled, "{}") {
		return fmt.Errorf("url_template %q has an unmatched brace", l.URLTemplate)
	}
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
