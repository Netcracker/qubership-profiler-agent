package insight

import (
	"context"
	"sort"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// ServicesInput selects the services to list.
type ServicesInput struct {
	Window Window
	// Filters may name namespace, service, and pod conditions only: /pods
	// takes no filter, so the client applies them to the list it gets back.
	Filters Filters
}

// ServicesResult lists the namespaces, services, and pods with profiling data
// in the window.
type ServicesResult struct {
	Window     ResolvedWindow `json:"window"`
	Namespaces []Namespace    `json:"namespaces"`
	// Partial names the sources that failed; the list may then be incomplete.
	Partial []string `json:"partial,omitempty"`
}

type Namespace struct {
	Name     string    `json:"name"`
	Services []Service `json:"services"`
}

type Service struct {
	Name string `json:"name"`
	Pods []Pod  `json:"pods"`
}

// Pod is one pod with each of its restarts that has data in the window.
type Pod struct {
	Name     string    `json:"name"`
	Restarts []Restart `json:"restarts"`
}

// Restart is one run of a pod: the JVM start time and the span of its calls.
type Restart struct {
	StartedAt time.Time `json:"started_at"`
	FirstCall time.Time `json:"first_call"`
	LastCall  time.Time `json:"last_call"`
}

type podsResponse struct {
	Pods           []model.PodEntry `json:"pods"`
	Partial        bool             `json:"partial"`
	PartialReasons []string         `json:"partial_reasons"`
}

// Services lists what has profiling data in the window: the first step of
// every investigation, so an agent learns the namespace and service names.
func (c *Client) Services(ctx context.Context, in ServicesInput) (ServicesResult, error) {
	window, err := in.Window.Resolve()
	if err != nil {
		return ServicesResult{}, err
	}
	conditions, err := in.Filters.conditions()
	if err != nil {
		return ServicesResult{}, err
	}
	match, err := identityMatcher(conditions)
	if err != nil {
		return ServicesResult{}, err
	}
	if err := c.authorize(ctx, namespaces(conditions)); err != nil {
		return ServicesResult{}, err
	}
	var resp podsResponse
	if err := c.getJSON(ctx, "/pods", window.values(), &resp); err != nil {
		return ServicesResult{}, err
	}

	byNamespace := map[string]map[string]map[string][]Restart{}
	for _, p := range resp.Pods {
		row := model.CallRow{PK: model.PK{PodNamespace: p.Namespace, PodService: p.Service, PodName: p.Pod}}
		if !match.Match(&row) {
			continue
		}
		services := byNamespace[p.Namespace]
		if services == nil {
			services = map[string]map[string][]Restart{}
			byNamespace[p.Namespace] = services
		}
		pods := services[p.Service]
		if pods == nil {
			pods = map[string][]Restart{}
			services[p.Service] = pods
		}
		pods[p.Pod] = append(pods[p.Pod], Restart{
			StartedAt: time.UnixMilli(p.RestartTimeMs).UTC(),
			FirstCall: time.UnixMilli(p.TimeMinMs).UTC(),
			LastCall:  time.UnixMilli(p.TimeMaxMs).UTC(),
		})
	}

	out := ServicesResult{Window: window, Namespaces: []Namespace{}}
	if resp.Partial {
		out.Partial = resp.PartialReasons
	}
	for _, ns := range sortedKeys(byNamespace) {
		n := Namespace{Name: ns}
		for _, svc := range sortedKeys(byNamespace[ns]) {
			s := Service{Name: svc}
			for _, pod := range sortedKeys(byNamespace[ns][svc]) {
				restarts := byNamespace[ns][svc][pod]
				sort.Slice(restarts, func(i, j int) bool { return restarts[i].StartedAt.Before(restarts[j].StartedAt) })
				s.Pods = append(s.Pods, Pod{Name: pod, Restarts: restarts})
			}
			n.Services = append(n.Services, s)
		}
		out.Namespaces = append(out.Namespaces, n)
	}
	return out, nil
}

// identityMatcher checks that every condition names an identity field and
// compiles them into a matcher over a row that carries only a PK.
func identityMatcher(conditions []string) (*model.Matcher, error) {
	q := model.CallsQuery{FromMs: 0, ToMs: 1}
	for _, text := range conditions {
		cond, err := model.ParseCondition(text)
		if err != nil {
			return nil, errors.Wrapf(err, "filter %q", text)
		}
		switch cond.Field {
		case "namespace", "service", "pod":
		default:
			return nil, errors.Errorf("filter %q: services takes namespace, service, and pod conditions only", text)
		}
		q.Filters = append(q.Filters, cond)
	}
	m, err := q.Matcher()
	return m, errors.Wrap(err, "filter")
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
