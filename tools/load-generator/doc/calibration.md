# Generator calibration runbook

The phase-2 exit criterion (`load-testing-plan.md` §3, `virtual-dumper.md` §6): one run compares the virtual
dumper's traffic profile against the real Java agent. Material divergence is fixed in the emulator, never tuned away
in the thresholds. The 2026-07-16 result is recorded in `load-testing-plan.md` §12.

## Tooling

`tools/load-generator/calibrate` is a decoding TCP tap: agents connect to it, it proxies to the collector and
decodes both directions into a JSON traffic profile (per-stream bytes/s in 1 s buckets, `RCV_DATA` size histogram,
flush/ack cadence, stream opens, reconnect timeline). `-inject-ack-error N` corrupts the Nth ack of the run into
`ACK_ERROR_MAGIC`, driving the connected agent — real or virtual — through its reconnect path with no collector
changes. `-compare a.json,b.json` checks two profiles against the pass criteria.

## Procedure

1. Start the dev stack: `cd backend && docker compose up --build -d` (collector on :1715).
2. Build the agent and the workload app once:
   `./gradlew --quiet :installer-zip-test:extractInstaller :test-app:jar`.
3. **Run A (reference)** — the real agent driving `LoadMain` (a steady programmatic workload; args:
   seconds, calls/s per thread, threads):

   ```bash
   cd backend
   go run ./tools/load-generator/calibrate -listen :1717 -target localhost:1715 -out run-a.json -run-for 165s &
   jar=$(ls ../test-app/build/libs/qubership-profiler-test-app-*.jar | grep -v sources | grep -v javadoc | tail -1)
   java -Dfile.encoding=UTF-8 \
     -javaagent:../installer-zip-test/build/profiler-home/lib/qubership-profiler-agent.jar \
     -Dprofiler.home=../installer-zip-test/build/profiler-home \
     -Dprofiler.config=tools/load-generator/calibrate/config/_config.xml \
     -DREMOTE_DUMP_HOST=localhost -DREMOTE_DUMP_PORT_PLAIN=1717 \
     -DCLOUD_NAMESPACE=load -DMICROSERVICE_NAME=load-agent \
     -cp "$jar" com.netcracker.profilerTest.testapp.LoadMain 120 5 3
   ```

4. **Run B** — the virtual dumper with knobs mirroring `LoadMain`:

   ```bash
   go run ./tools/load-generator/calibrate -listen :1716 -target localhost:1715 -out run-b.json -run-for 135s &
   go run ./tools/load-generator/feeder -addr localhost:1716 -pods 1 -threads 3 -calls-per-sec 4.07 \
     -dict-initial 60 -dict-growth-per-min 0 -duration-thresholds "100ms,1s" -duration-shares "0.90,0.08,0.02" \
     -stack-depth 2 -sql-share 0 -xml-share 0 -suspend-rate 4 -error-share 0 -duration 120s
   ```

5. Compare: `go run ./tools/load-generator/calibrate -compare run-a.json,run-b.json`.
6. **Reconnect check** — repeat both runs with `-inject-ack-error 150` (pick an ack index that lands mid-run) and
   compare again: both sides must show a follow-up connection re-opening all seven streams with the dictionary
   `resetRequired=1`.

## Mirroring notes

- `LoadMain`'s threads block for the call duration, so its effective rate sits below the nominal one (long calls eat
  slots). The virtual dumper deliberately does not model occupancy (`virtual-dumper.md` §4) — mirror the *observed*
  calls/s (run A's record count / duration / threads), not the nominal flag value.
- `-dict-initial` mirrors the agent's actual tag count (LoadMain registers ~55 words: per-thread methods plus params
  and agent-internal tags).
- The real JVM's suspend stream carries genuine timer hiccups at a host-dependent rate (~4/s on the dev laptop);
  mirror what run A measured.
- `params` and `sql` sit under the comparator's 20 B/s noise floor (one-shot / trickle streams); their ratios are
  reported as informational.
- The reconnect gap reported for run A is bookkeeping-skewed: the tap closes the injected connection only when the
  agent's socket dies, while the agent may stall on the half-dead socket for up to its 30 s read timeout before the
  10 s restart sleep. Judge run A by the follow-up connection's stream opens, not by the printed gap.
