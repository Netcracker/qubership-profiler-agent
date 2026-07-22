// Command calibrate is the phase-2 fidelity harness (virtual-dumper.md §6):
// a decoding TCP tap between an agent and a collector, plus a profile
// comparator.
//
// Tap mode proxies agent ↔ collector, decodes both directions of the wire
// protocol, and writes a JSON traffic profile: bytes/s per stream in 1 s
// buckets, RCV_DATA size histogram, flush/ack cadence, stream opens, and the
// reconnect timeline. It can also corrupt one chosen ack into
// ACK_ERROR_MAGIC, which drives the *real* agent through its reconnect path
// with no collector changes:
//
//	go run ./tools/load-generator/calibrate -listen :1716 -target localhost:1715 \
//	    -out run-a.json -run-for 3m [-inject-ack-error 500]
//
// Compare mode checks two profiles against the §6 pass criteria:
//
//	go run ./tools/load-generator/calibrate -compare run-a.json,run-b.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	var (
		listen   = flag.String("listen", ":1716", "tap listen address (agents connect here)")
		target   = flag.String("target", "localhost:1715", "collector agent address")
		out      = flag.String("out", "profile.json", "traffic-profile output path")
		runFor   = flag.Duration("run-for", 0, "stop after this long; 0 runs until SIGINT")
		inject   = flag.Int("inject-ack-error", 0, "corrupt the Nth ack (1-based, across the run) into ACK_ERROR_MAGIC; 0 disables")
		compare  = flag.String("compare", "", "compare two profile JSONs (comma-separated) instead of tapping")
		tolerate = flag.Float64("tolerance", 1.5, "max per-stream bytes/s ratio in compare mode")
	)
	flag.Parse()

	if *compare != "" {
		parts := strings.Split(*compare, ",")
		if len(parts) != 2 {
			fmt.Fprintln(os.Stderr, "-compare wants exactly two paths: a.json,b.json")
			os.Exit(2)
		}
		os.Exit(runCompare(parts[0], parts[1], *tolerate))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *runFor > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *runFor)
		defer cancel()
	}

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("calibrate: tapping %s -> %s, profile -> %s\n", *listen, *target, *out)

	tap := newTap(*target, *inject)
	var conns sync.WaitGroup
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			break // listener closed on ctx.Done
		}
		conns.Add(1)
		go func() {
			defer conns.Done()
			tap.handle(ctx, c)
		}()
	}
	conns.Wait()

	data, err := json.MarshalIndent(tap.profile(), "", " ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("calibrate: wrote %s (%d connections)\n", *out, len(tap.profile().Connections))
}

// nowMs is the tap's single time source, kept trivial on purpose.
func nowMs() int64 { return time.Now().UnixMilli() }
