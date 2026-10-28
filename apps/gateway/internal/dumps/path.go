package dumps

import (
	"strings"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
)

// pathSegments is the number of segments after the prefix in a diagtools
// upload URL: {namespace}/{yyyy}/{MM}/{dd}/{HH}/{mm}/{ss}/{pod}/{file}.
const pathSegments = 9

// underPrefix reports whether p is prefix itself or below it.
func underPrefix(p, prefix string) bool {
	prefix = strings.TrimSuffix(prefix, "/")
	return p == prefix || strings.HasPrefix(p, prefix+"/")
}

// parsePath extracts the rule attributes from an upload path. A path that
// does not have the diagtools layout yields empty attributes, which match
// only rules without keys.
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
