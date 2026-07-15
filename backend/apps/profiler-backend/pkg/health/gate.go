// Package health tracks the 03-lifecycle.md §2 startup state machine and
// serves the readiness and liveness probes in front of a service's HTTP API.
package health

import (
	"encoding/json"
	"net/http"
	"sync"
)

// State is one 03-lifecycle.md §2 lifecycle state.
type State string

// The §2 state names; the probe body carries them verbatim (§4).
const (
	StateInit        State = "INIT"
	StateLoading     State = "LOADING"
	StateRecovery    State = "RECOVERY"
	StateReady       State = "READY"
	StateDraining    State = "DRAINING"
	StateTerminating State = "TERMINATING"
	StateFatal       State = "FATAL"
)

// Gate serves <prefix>/health/ready and <prefix>/health/live around an API
// handler that is mounted only once the service reaches READY. Binding the
// gate before recovery lets a probe watch the LOADING → RECOVERY → READY
// progress while the API routes answer 503 (03-lifecycle.md §2, §4).
type Gate struct {
	prefix string

	mu      sync.RWMutex
	state   State
	details string
	api     http.Handler
}

// NewGate creates a gate for the given route prefix ("/internal/v1" for the
// collector, "/api/v1" for query), starting in INIT.
func NewGate(prefix string) *Gate {
	return &Gate{prefix: prefix, state: StateInit}
}

// Set moves the state machine; details show up in the probe body for humans
// (kubelet only reads the status code, §4).
func (g *Gate) Set(state State, details string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state, g.details = state, details
}

// State reports the current lifecycle state.
func (g *Gate) State() State {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state
}

// Mount installs the API handler the gate fronts. Call it right before
// flipping to READY.
func (g *Gate) Mount(api http.Handler) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.api = api
}

func (g *Gate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.RLock()
	state, details, api := g.state, g.details, g.api
	g.mu.RUnlock()

	switch r.URL.Path {
	case g.prefix + "/health/ready":
		status := http.StatusServiceUnavailable
		if state == StateReady {
			status = http.StatusOK
		}
		writeState(w, status, state, details)
	case g.prefix + "/health/live":
		// §4: most failures must fail readiness, not liveness — a liveness
		// failure earns a kubelet kill, so only FATAL reports one.
		status := http.StatusOK
		if state == StateFatal {
			status = http.StatusServiceUnavailable
		}
		writeState(w, status, state, details)
	default:
		if api == nil {
			writeState(w, http.StatusServiceUnavailable, state, details)
			return
		}
		api.ServeHTTP(w, r)
	}
}

func writeState(w http.ResponseWriter, status int, state State, details string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body := map[string]string{"state": string(state)}
	if details != "" {
		body["details"] = details
	}
	_ = json.NewEncoder(w).Encode(body)
}
