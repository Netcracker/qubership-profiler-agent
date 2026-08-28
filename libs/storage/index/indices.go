package index

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
)

// -----------------------------------------------------------------------------
// IdxVal represents a single value-to-file mapping in the inverted index.
// Each entry associates a specific value (e.g., request_id) with a file UUID.
// -----------------------------------------------------------------------------

type IdxVal struct { // value for inverted index
	Value  string
	FileId string
}

func (ii *IdxVal) String() string {
	return fmt.Sprintf("IdxVal{Value: %s, FileId: %s}\n", ii.Value, ii.FileId)
}

// -----------------------------------------------------------------------------
// Map is an in-memory inverted index structure.
//
// It stores parameter names mapped to lists of IdxVal records.
// Only parameters from the importantParams whitelist are indexed.
// -----------------------------------------------------------------------------

type Map struct {
	mu              sync.RWMutex         // protects access to Indexes
	Indexes         map[string][]*IdxVal // inverted index: paramName -> list of (value, file_id)
	importantParams map[string]bool      // allowlist of allowed param names to index
}

// NewMap creates a new inverted index structure with the given allowlist of parameters.
func NewMap(importantParams map[string]bool) *Map {
	return &Map{
		Indexes:         make(map[string][]*IdxVal),
		importantParams: importantParams,
	}
}

// SkipParam returns true if the parameter is not in the allowlist.
func (im *Map) SkipParam(paramName string) bool {
	if _, ok := im.importantParams[paramName]; ok {
		return false
	}
	return true
}

// AddValues adds multiple values for a single parameter and file ID.
// Only allowlisted parameters are accepted.
func (im *Map) AddValues(fileUuid common.Uuid, paramName string, values []string) {
	if im.SkipParam(paramName) {
		return // skip non-important params
	}
	im.mu.Lock()
	defer im.mu.Unlock()

	for _, value := range values {
		im.addParameter(fileUuid.Str, paramName, value)
	}
}

// addParameter adds a single (value, file_id) entry to the index under paramName.
func (im *Map) addParameter(fileId, paramName, value string) {
	// double check that param is important (could be called directly)
	if _, has := im.importantParams[paramName]; !has {
		return
	}
	// initialize slice if not present
	if _, ok := im.Indexes[paramName]; !ok {
		im.Indexes[paramName] = []*IdxVal{}
	}
	// append the new entry
	im.Indexes[paramName] = append(im.Indexes[paramName],
		&IdxVal{
			Value:  value,
			FileId: fileId,
		},
	)
}

// ParametersCount returns the number of distinct parameters indexed.
func (im *Map) ParametersCount() int {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return len(im.Indexes)
}

// Parameters return a sorted list of parameter names present in the index.
func (im *Map) Parameters() []string {
	im.mu.RLock()
	defer im.mu.RUnlock()
	list := make([]string, 0, len(im.Indexes))
	for k, _ := range im.Indexes {
		list = append(list, k)
	}
	sort.Strings(list)
	return list
}

// String returns the raw string representation of the index contents.
func (im *Map) String() string {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return fmt.Sprintf("IndexMap{%v}", im.Indexes)
}

type InvertedIndexConfig struct {
	Granularity time.Duration
	Lifetime    time.Duration
	Params      []string
	Prefixes    []string
}
