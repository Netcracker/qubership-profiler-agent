package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// podInfo is the slice of pod status §8.8 needs: identity plus the summed
// container restart count.
type podInfo struct {
	Name     string
	UID      string
	Restarts int
}

// podLister lists the backend pods under watch. The production
// implementation talks to the k8s API; tests use a fake.
type podLister interface {
	list(ctx context.Context) ([]podInfo, error)
}

// kubeLister resolves the client the way tools/migration does: kubeconfig
// (KUBECONFIG or the default home path) first, in-cluster config as the
// fallback for a checker running as a pod.
type kubeLister struct {
	client    kubernetes.Interface
	namespace string
	selector  string
}

func newKubeLister(namespace, selector string) (*kubeLister, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("no kubeconfig and no in-cluster config: %w", err)
		}
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &kubeLister{client: client, namespace: namespace, selector: selector}, nil
}

func (k *kubeLister) list(ctx context.Context) ([]podInfo, error) {
	pods, err := k.client.CoreV1().Pods(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: k.selector})
	if err != nil {
		return nil, err
	}
	out := make([]podInfo, 0, len(pods.Items))
	for _, p := range pods.Items {
		restarts := 0
		for _, cs := range p.Status.ContainerStatuses {
			restarts += int(cs.RestartCount)
		}
		out = append(out, podInfo{Name: p.Name, UID: string(p.UID), Restarts: restarts})
	}
	return out, nil
}

// podTrack follows one pod UID across the run.
type podTrack struct {
	name string
	// baseline is the restart count at first sight: the first successful
	// list for baseline pods, zero for replacements (their own restarts all
	// count, plus the +1 replacement event).
	baseline      int
	last          int
	alive         bool
	isReplacement bool
}

// restartEvent is one observed restart-count increment or replacement, with
// the pod-list time it was first seen at — the time allowance windows match
// against (doc/checker.md, "Expected failures").
type restartEvent struct {
	pod        string
	observedAt time.Time
	weight     int
	kind       string // restart | replacement
}

// podState is the §8.8 accounting (doc/checker.md): every restart-count
// increment and replacement becomes a timed event; events matched to a
// per-injection restarts allowance (one event per injection, target pod
// only) are expected, everything else counts against -allowed-restarts.
type podState struct {
	allowed int

	baselined bool
	tracks    map[string]*podTrack
	events    []restartEvent
	gone      []string // names of pods that vanished, in vanish order
	current   func(faults *faultState) []finding
	replaced  int
}

func newPodState(allowed int) *podState {
	p := &podState{allowed: allowed, tracks: map[string]*podTrack{}}
	p.current = func(*faultState) []finding { return nil } // nothing observed yet
	return p
}

func (p *podState) findings(faults *faultState) []finding { return p.current(faults) }

// observe folds one successful pod list into the accounting.
func (p *podState) observe(pods []podInfo, at time.Time) {
	seen := map[string]bool{}
	for _, pod := range pods {
		seen[pod.UID] = true
		track, ok := p.tracks[pod.UID]
		if !ok {
			track = &podTrack{name: pod.Name, isReplacement: p.baselined}
			if !p.baselined {
				track.baseline = pod.Restarts
			}
			p.tracks[pod.UID] = track
			if p.baselined {
				p.replaced++
				p.events = append(p.events, restartEvent{pod: pod.Name, observedAt: at, weight: 1, kind: "replacement"})
			}
		}
		if delta := pod.Restarts - track.last; delta > 0 && p.baselined {
			p.events = append(p.events, restartEvent{pod: pod.Name, observedAt: at, weight: delta, kind: "restart"})
		}
		track.last = pod.Restarts
		track.alive = true
	}
	for uid, track := range p.tracks {
		if track.alive && !seen[uid] {
			track.alive = false
			p.gone = append(p.gone, track.name)
		}
	}
	p.baselined = true
	p.current = p.evaluate
}

// evaluate classifies the accumulated events against the allowances. It
// recomputes from the full event list every tick: events and windows only
// append, so the greedy earliest-window matching stays deterministic.
func (p *podState) evaluate(faults *faultState) []finding {
	var out []finding

	// consumed tracks per-injection budgets: each injection grants its
	// declared restartBudget of restart-or-replacement units (default 1;
	// a grace-0 collector kill measures at 2 — replacement plus one
	// collector.lock-collision container restart). Excess stays a violation.
	consumed := map[string]int{}
	unexpectedTotal := 0
	var unexpected []string
	expectedByFault := map[string][]string{}
	for _, ev := range p.events {
		remaining := ev.weight
		if faults != nil {
			for _, w := range faults.restartAllowances(ev.pod) {
				if remaining == 0 {
					break
				}
				if consumed[w.faultID] >= w.budget || !w.contains(ev.observedAt) {
					continue
				}
				consumed[w.faultID]++
				remaining--
				expectedByFault[w.faultID] = append(expectedByFault[w.faultID],
					fmt.Sprintf("%s %s", ev.pod, ev.kind))
			}
		}
		if remaining > 0 {
			unexpectedTotal += remaining
			unexpected = append(unexpected, fmt.Sprintf("%s %s +%d at %s",
				ev.pod, ev.kind, remaining, ev.observedAt.Format(time.RFC3339)))
		}
	}
	if unexpectedTotal > p.allowed {
		sort.Strings(unexpected)
		out = append(out, finding{subject: "restart-budget", observedAt: lastEventTime(p.events),
			msg: fmt.Sprintf("%d restart events exceed the budget of %d: %s",
				unexpectedTotal, p.allowed, strings.Join(unexpected, ", "))})
	}
	for faultID, evs := range expectedByFault {
		sort.Strings(evs)
		out = append(out, finding{subject: "restart-budget", expected: true,
			observedAt: lastEventTime(p.events),
			msg:        fmt.Sprintf("expected under fault %s: %s", faultID, strings.Join(evs, ", "))})
	}

	if excess := len(p.gone) - p.replaced; excess > 0 {
		names := p.gone[len(p.gone)-excess:]
		out = append(out, finding{subject: "pods-gone", observedAt: lastEventTime(p.events),
			msg: fmt.Sprintf("%d pod(s) disappeared without a replacement: %s", excess, strings.Join(names, ", "))})
	}
	return out
}

func lastEventTime(events []restartEvent) time.Time {
	if len(events) == 0 {
		return time.Time{}
	}
	return events[len(events)-1].observedAt
}
