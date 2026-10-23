package model

import (
	"strings"
)

// The derived fields of 02 §2.3.3: `class` from the method text, `endpoint`
// from the web.method and web.url params. The filter and the /stats grouping
// both read them through these functions, so a grouping key always selects
// its row's calls as a condition.

// CallClass returns the declaring class of a call's root method: the
// qualified name before the first `(`, without its last `.`-separated part.
// It returns "" for a method text that names no class, such as the `#<id>`
// placeholder of a method missing from the dictionary.
func CallClass(method string) string {
	head, _, _ := strings.Cut(method, "(")
	words := strings.Fields(head)
	if len(words) == 0 {
		return ""
	}
	name := words[len(words)-1]
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 {
		return ""
	}
	return name[:dot]
}

// The params the endpoint field reads.
const (
	ParamWebURL    = "web.url"
	ParamWebMethod = "web.method"
)

// CallEndpoint returns `<web.method> <path>`, or the path alone without
// web.method. ok is false when the call has no web.url. A param with several
// values contributes its first, as the UI's call title does.
func CallEndpoint(params map[string][]string) (endpoint string, ok bool) {
	urls := params[ParamWebURL]
	if len(urls) == 0 {
		return "", false
	}
	path := endpointPath(urls[0])
	if methods := params[ParamWebMethod]; len(methods) > 0 && methods[0] != "" {
		return methods[0] + " " + path, true
	}
	return path, true
}

// endpointPath strips the scheme, the authority, the query string, and the
// fragment from a URL, and replaces each identifier segment with `{id}`.
func endpointPath(raw string) string {
	path := raw
	if i := strings.Index(path, "://"); i >= 0 {
		rest := path[i+3:]
		end := strings.IndexAny(rest, "/?#")
		if end < 0 || rest[end] != '/' {
			path = "/"
		} else {
			path = rest[end:]
		}
	}
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	if path == "" {
		path = "/"
	}
	segments := strings.Split(path, "/")
	for i, s := range segments {
		if isIDSegment(s) {
			segments[i] = "{id}"
		}
	}
	return strings.Join(segments, "/")
}

// isIDSegment reports a path segment that identifies one resource: all
// digits, a UUID, or 16 or more hex digits. Replacing these keeps the number
// of distinct endpoints small enough to group by.
func isIDSegment(s string) bool {
	if s == "" {
		return false
	}
	if strings.Trim(s, "0123456789") == "" {
		return true
	}
	if len(s) >= 16 && isHex(s) {
		return true
	}
	return isUUID(s)
}

func isHex(s string) bool {
	return strings.Trim(s, "0123456789abcdefABCDEF") == ""
}

// isUUID matches the 8-4-4-4-12 hex form.
func isUUID(s string) bool {
	parts := strings.Split(s, "-")
	if len(parts) != 5 {
		return false
	}
	for i, n := range []int{8, 4, 4, 4, 12} {
		if len(parts[i]) != n || !isHex(parts[i]) {
			return false
		}
	}
	return true
}
