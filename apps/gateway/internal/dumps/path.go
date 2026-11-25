package dumps

import (
	"net/url"
	"path"
	"strings"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
)

// canonicalPath reports whether u's path is already in the form an upstream
// such as nginx normalizes it to. Such a path has no empty, ".", or ".."
// segment, and no percent-encoding that differs from the default one, such
// as %2F. The proxy forwards the request URI unchanged, so for a canonical
// path the rules and the upstream read the same namespace, pod, and file.
func canonicalPath(u *url.URL) bool {
	if u.RawPath != "" {
		return false
	}
	c := path.Clean(u.Path)
	return u.Path == c || u.Path == c+"/"
}

// pathSegments is the number of segments after the prefix in a diagtools
// upload URL: {namespace}/{yyyy}/{MM}/{dd}/{HH}/{mm}/{ss}/{pod}/{file}.
const pathSegments = 9

// underPrefix reports whether p is prefix itself or below it.
func underPrefix(p, prefix string) bool {
	prefix = strings.TrimSuffix(prefix, "/")
	return p == prefix || strings.HasPrefix(p, prefix+"/")
}

// parsePath extracts the rule attributes from an upload path. A path that
// does not have the diagtools layout yields empty attributes: a rule matches
// them only if it sets no dump_type and each of its globs matches "", as "*"
// does.
func parsePath(p, prefix string) rules.Attrs {
	rest := strings.Trim(strings.TrimPrefix(p, strings.TrimSuffix(prefix, "/")), "/")
	parts := strings.Split(rest, "/")
	if len(parts) != pathSegments {
		return rules.Attrs{}
	}
	pod := parts[7]
	// diagtools reads the pod name from a file that can carry a
	// `_<start timestamp>` suffix. Kubernetes pod names cannot contain an
	// underscore, so everything before the first one is the pod name.
	if i := strings.IndexByte(pod, '_'); i >= 0 {
		pod = pod[:i]
	}
	return rules.Attrs{
		Namespace: parts[0],
		Pod:       pod,
		DumpType:  dumpType(parts[8]),
	}
}

// dumpType classifies an uploaded file by the names diagtools gives it.
func dumpType(file string) string {
	switch {
	case strings.Contains(file, ".hprof"):
		return rules.DumpTypeHeap
	case strings.HasSuffix(file, ".td.txt"):
		return rules.DumpTypeThread
	case file == "gc.log" || strings.HasPrefix(file, "gc.log."):
		return rules.DumpTypeGC
	default:
		return rules.DumpTypeOther
	}
}
