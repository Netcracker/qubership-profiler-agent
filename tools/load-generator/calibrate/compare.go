package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// runCompare checks two tap profiles against the §6 pass criteria: per-stream
// bytes/s within the tolerance, a matching flush cadence, and — when an ack
// error was injected on both sides — a matching reconnect shape (a second
// connection re-opening all streams with the dictionary reset). Returns the
// process exit code.
func runCompare(pathA, pathB string, tolerance float64) int {
	a, err := loadProfile(pathA)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	b, err := loadProfile(pathB)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	pass := true
	fmt.Printf("A: %s (%d connections)   B: %s (%d connections)\n\n",
		pathA, len(a.Connections), pathB, len(b.Connections))

	ra, rb := ratesOf(a), ratesOf(b)
	streams := map[string]bool{}
	for s := range ra {
		streams[s] = true
	}
	for s := range rb {
		streams[s] = true
	}
	names := make([]string, 0, len(streams))
	for s := range streams {
		names = append(names, s)
	}
	sort.Strings(names)

	// One-shot and trickle streams (params, a near-empty sql) sit under this
	// floor; their ratios are noise, not a traffic-profile signal.
	const noiseFloor = 20.0 // bytes/s

	fmt.Printf("%-12s %14s %14s %8s\n", "stream", "A bytes/s", "B bytes/s", "ratio")
	for _, s := range names {
		va, vb := ra[s], rb[s]
		ratio := 0.0
		switch {
		case va > 0 && vb > 0:
			ratio = va / vb
			if ratio < 1 {
				ratio = vb / va
			}
		case va != vb:
			ratio = tolerance + 1 // one side missing the stream entirely
		}
		verdict := "ok"
		switch {
		case va < noiseFloor && vb < noiseFloor:
			verdict = "low-volume, informational"
		case ratio > tolerance:
			verdict = "DIVERGES"
			pass = false
		}
		fmt.Printf("%-12s %14.1f %14.1f %7.2fx %s\n", s, va, vb, ratio, verdict)
	}

	fa, fb := flushPer5s(a), flushPer5s(b)
	fmt.Printf("\nREQUEST_ACK_FLUSH per 5 s: A %.1f, B %.1f\n", fa, fb)
	if fa == 0 || fb == 0 || fa/fb > tolerance || fb/fa > tolerance {
		fmt.Println("flush cadence DIVERGES")
		pass = false
	}

	if err := checkReconnect(a, "A"); err != nil {
		fmt.Println(err)
		pass = false
	}
	if err := checkReconnect(b, "B"); err != nil {
		fmt.Println(err)
		pass = false
	}

	if pass {
		fmt.Println("\nPASS: profiles match within tolerance")
		return 0
	}
	fmt.Println("\nFAIL: material divergence — fix the emulator, not the thresholds (load-testing-plan.md §3)")
	return 1
}

func loadProfile(path string) (*Profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p Profile
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(p.Connections) == 0 {
		return nil, fmt.Errorf("%s: no connections captured", path)
	}
	return &p, nil
}

// ratesOf sums per-stream bytes over every connection and divides by the
// total connected time — steady-state bytes/s.
func ratesOf(p *Profile) map[string]float64 {
	var totalSec float64
	bytes := map[string]int64{}
	for _, c := range p.Connections {
		totalSec += float64(c.EndMs-c.StartMs) / 1000
		for s, st := range c.Streams {
			bytes[s] += st.Bytes
		}
	}
	out := map[string]float64{}
	if totalSec == 0 {
		return out
	}
	for s, n := range bytes {
		out[s] = float64(n) / totalSec
	}
	return out
}

// flushPer5s is the observed REQUEST_ACK_FLUSH count normalized to the
// agent's 5 s flush window.
func flushPer5s(p *Profile) float64 {
	var count, sec float64
	for _, c := range p.Connections {
		for _, n := range c.AckFlushSec {
			count += float64(n)
		}
		sec += float64(c.EndMs-c.StartMs) / 1000
	}
	if sec == 0 {
		return 0
	}
	return count / sec * 5
}

// checkReconnect verifies the reconnect shape after an injected ack error:
// a follow-up connection exists and re-opens the dictionary with
// resetRequired=1.
func checkReconnect(p *Profile, label string) error {
	injectedAt := -1
	for i, c := range p.Connections {
		if c.Injected {
			injectedAt = i
			break
		}
	}
	if injectedAt == -1 {
		return nil // no injection on this side; nothing to check
	}
	if injectedAt == len(p.Connections)-1 {
		return fmt.Errorf("%s: ack error injected but no reconnect followed", label)
	}
	next := p.Connections[injectedAt+1]
	for _, init := range next.Inits {
		if init.Stream == "dictionary" {
			if !init.Reset {
				return fmt.Errorf("%s: the post-reconnect dictionary open lacks resetRequired=1", label)
			}
			fmt.Printf("%s: reconnect after injected ACK_ERROR re-opened %d streams, dictionary reset ok (gap %d ms)\n",
				label, len(next.Inits), next.StartMs-p.Connections[injectedAt].EndMs)
			return nil
		}
	}
	return fmt.Errorf("%s: the reconnect did not re-open the dictionary", label)
}
