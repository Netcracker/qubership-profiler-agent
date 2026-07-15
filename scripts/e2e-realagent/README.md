# Real-agent E2E harness

Drives the actual Java profiler agent against the Go backend. Two variants share one Go harness (`backend/libs/tests/smoke_realagent/harness.go`) but obtain the agent differently and check different things:

- **Byte-exactness** (`realagent_test.go`, tag `smoke_realagent`) — builds the agent from whatever is checked out at HEAD and asserts adversarial method/parameter strings round-trip byte-exact. This is the acceptance gate for the decoder fixes below.
- **Legacy `gc` stream** (`realagent_v313_test.go`, tag `smoke_realagent_v313`) — downloads the pre-built v3.1.3 release and asserts its calls land in `/api/v1/calls` at all. See [its own section](#v313-variant-the-legacy-gc-stream) below.

Everything is plain Go — no shell script. `harness.go` runs `java -javaagent:...` via `os/exec` directly; the byte-exactness variant drives `gradlew`/`gradlew.bat` the same way. Both run unmodified on Windows, Linux, and macOS.

## Byte-exactness variant

The test **fails today** on two backend decoder bugs. That failing assertion is the deliverable — do not change decoder code to make it pass.

### What it exercises

The existing Go smoke test (`backend/libs/tests/smoke`) feeds the collector with bytes from a Go emulator. The emulator's own encoder mirrors the buggy decoder, so it cannot surface these bugs. This harness sends bytes from the **real agent**, whose `DataOutputStreamEx.writeChars` writes faithful UTF-16 — which is what exposes them.

- **Bug A — `readChar` signedness** (`backend/libs/parser/pipe/pipe_reader.go`). The reader reads a signed `int16`, so every UTF-16 code unit `>= U+8000` (most CJK and Hangul) and both halves of a non-BMP surrogate pair (emoji) decode to `U+FFFD`. It corrupts every string: method names, class names, parameter keys, parameter values, thread names.
- **Bug B — empty dictionary word** (`backend/libs/parser/pipe/dictionary.go`). The reader skips an empty word without advancing its id counter, and the collector appends words by arrival order, so every later id shifts down by one and resolves to the wrong method or parameter name.

### Moving parts

| File | Role |
| --- | --- |
| `test-app/src/main/java/com/netcracker/profilerTest/testapp/AdversarialMain.java` | The workload. Records two synthetic calls through the `Profiler` API: Call A carries adversarial Unicode (bug A); Call B resolves an empty dictionary word first, then plain-ASCII names (bug B). |
| `scripts/e2e-realagent/config/_config.xml` | Profiler config. Marks the test-app package `do-not-profile` so the only recorded calls are the two synthetic ones. |
| `backend/libs/tests/smoke_realagent/realagent_test.go` | The `//go:build smoke_realagent` test. Runs `gradlew`/`gradlew.bat` to build the agent + test-app jar (`buildHeadAgent`), runs the workload via the shared `runJavaAgent`, polls `/api/v1/calls`, fetches each call's `/tree`, and asserts the strings byte-exact. |
| `backend/libs/tests/smoke_realagent/harness.go` | Shared by both variants: `runJavaAgent` (the `-javaagent` invocation), `pollNamespaceCalls`, `waitReady`, `repoRoot`. |

### Prerequisites

- A JDK 17+ and the repo's Gradle build (the harness builds the agent).
- Docker, for the backend `docker-compose` stack.

### Run it

From `backend/`, one shot — brings the stack up, runs the test, tears the stack down:

```bash
make -C backend smoke-realagent
```

Or step by step, keeping the stack up for iteration:

```bash
cd backend
docker compose up --build -d
go test -tags smoke_realagent -count=1 -timeout 20m -v ./libs/tests/smoke_realagent/...
docker compose down -v --remove-orphans
```

Environment knobs (all optional):

| Variable | Default | Meaning |
| --- | --- | --- |
| `REALAGENT_AGENT_ADDR` | `localhost:1715` | Collector `host:port` the agent connects to. |
| `REALAGENT_QUERY_URL` | `http://localhost:8080` | Query API base URL. |
| `REALAGENT_INTERNAL_URL` | `http://localhost:8081` | Internal (collector health) API base URL. |
| `REALAGENT_NAMESPACE` | `e2e-realagent` | Pod namespace the agent reports. |
| `REALAGENT_SERVICE` | `adversarial-app` | Service the agent reports. |
| `SKIP_BUILD` | unset | Set to `1` to reuse an already-built agent and jar (skips the `gradlew` step). |

## How the agent reaches the collector

The agent switches from local-file dumps to the TCP collector purely because `REMOTE_DUMP_HOST` is set. From `dumper/.../Dumper.java`:

```text
remoteConfigured  = isNotEmpty(REMOTE_DUMP_HOST)
localDumpEnabled  = forceLocalDump || !remoteConfigured
```

The plain port defaults to `ProtocolConst.PLAIN_SOCKET_PORT` (1715) when `REMOTE_DUMP_PORT_SSL` is unset; the harness also passes `REMOTE_DUMP_PORT_PLAIN` for clarity. Namespace and service come from `CLOUD_NAMESPACE` / `MICROSERVICE_NAME` (`VariableFinder`); the pod name is the host name plus a timestamp, so the test matches calls by namespace and service rather than by pod name.

Both keys are read as either JVM system properties (`-D…`) or environment variables (`PropertyFacadeBoot.getPropertyOrEnvVariable`). The harness passes them as `-D` flags.

The agent also needs its plugin jars. It auto-detects them from `${profiler.home}/lib`, so the harness sets `-Dprofiler.home` to the extracted installer home while keeping `-Dprofiler.config` on the in-tree config for whichever variant is running.

## Expected failure (byte-exactness variant)

Six assertions fail, three per bug.

Bug A — Call A:

```text
method:      void com.acme.Svc.語한🔥_handle() ...   ->   void com.acme.Svc.����_handle() ...
param key:   param.語한🔥                             ->   param.����
param value: value-語한🔥-tail                        ->   value-����-tail
```

Bug B — Call B, where a single empty word shifts every later id by one:

```text
method:      void com.acme.Svc.plainAsciiHandleB() ...   ->   param.b.alpha
param.b.alpha -> value-b-alpha                            ->   value-b-alpha bound under param.b.beta
param.b.beta  -> value-b-beta                             ->   value-b-alpha
```

## What a fix needs to touch

- Bug A: `backend/libs/parser/pipe/pipe_reader.go` — `readChar` must read an unsigned 16-bit code unit and reassemble surrogate pairs into the full code point, matching the Java reference (`DictionaryPhraseReader` / `DataInputStreamEx.readString`).
- Bug B: `backend/libs/parser/pipe/dictionary.go` — `DictionaryPipeReader` must register every word, including the empty string, so ids stay aligned with the agent (`DictionaryPhraseReader` registers every entry).

## v3.1.3 variant: the legacy `gc` stream

A second, separate harness is the regression gate for a different bug: agents built before v3.1.4 register an eighth `gc` stream unconditionally whenever they stream directly to a collector, regardless of whether GC-log harvesting is even enabled (`Dumper.java`'s `gcOs`, deleted in commit `ac804ee3` together with `GCDumper` when GC-log collection moved to `diagtools`).
Before the fix in `backend/libs/protocol/streams.go` / `backend/libs/collector/ingest/streams.go`, the collector treated `gc` as an unknown stream and tore the WHOLE connection down on it — so a pre-v3.1.4 agent wrote no data at all, not just its GC-log bytes.

Because v3.1.4 removed the `gc` stream entirely, HEAD can no longer reproduce the bug. Rather than building the agent from the `v3.1.3` git tag (a full old-tag Gradle build, including every plugin), `realagent_v313_test.go` downloads the pre-built v3.1.3 release straight from Maven Central:
`org.qubership.profiler:qubership-profiler-installer:3.1.3` (the zip — the exact `lib/` + `config/` layout `extractInstaller` produces locally, including the shaded `lib/qubership-profiler-runtime.jar` that actually carries `Dumper`/`GCDumper`) and `org.qubership.profiler:qubership-profiler-test-app:3.1.3` (the plain jar).
Both downloads are verified against Maven Central's published SHA-1 sidecars.
It runs the v3.1.3 test-app's plain `Main` class (that tag predates `AdversarialMain`) under a config (`config/_config-v313.xml`) that turns bytecode instrumentation ON for the test-app package — the opposite of `_config.xml`'s `<do-not-profile/>`, since `Main` relies on ordinary instrumentation rather than the programmatic `Profiler` API.

Note: `qubership-profiler-runtime`'s own plain Maven Central jar is a near-empty aggregator (the `runtime` module has no sources of its own — it only shades `dumper` + `instrumenter` together via `com.gradleup.shadow`, and the `java-published-library` convention publishes the plain, unshaded jar).
The functional 14 MB+ shaded jar only exists inside the `qubership-profiler-installer` distribution zip, which is why this harness fetches that zip rather than assembling `lib/` from the individual module artifacts.

| File | Role |
| --- | --- |
| `backend/libs/tests/smoke_realagent/realagent_v313_test.go` | The `//go:build smoke_realagent_v313` test. Downloads the v3.1.3 installer zip + test-app jar from Maven Central (`fetchV313Agent`), runs the old `Main` class via the shared `runJavaAgent`, then polls `/api/v1/calls` and asserts ≥1 call from this run's namespace/service landed, proving the connection survived the `gc` stream. |
| `scripts/e2e-realagent/config/_config-v313.xml` | Profiler config. Enables instrumentation for `com.netcracker.profilerTest.testapp.**` (default is off until a rule opts a class in). |

Run it (needs Docker for the backend stack and a JRE to run the downloaded agent — no JDK or Gradle build required for this variant):

```bash
make -C backend smoke-realagent-v313
```

or step by step:

```bash
cd backend
docker compose up --build -d
go test -tags smoke_realagent_v313 -count=1 -timeout 20m -v ./libs/tests/smoke_realagent/...
docker compose down -v --remove-orphans
```

Environment knobs (all optional):

| Variable | Default | Meaning |
| --- | --- | --- |
| `REALAGENT_V313_AGENT_ADDR` | `localhost:1715` | Collector `host:port` the agent connects to. |
| `REALAGENT_V313_QUERY_URL` | `http://localhost:8080` | Query API base URL. |
| `REALAGENT_V313_INTERNAL_URL` | `http://localhost:8081` | Internal (collector health) API base URL. |
| `REALAGENT_V313_NAMESPACE` | `e2e-realagent-v313` | Pod namespace the agent reports. |
| `REALAGENT_V313_SERVICE` | `legacy-gc-app` | Service the agent reports. |
| `AGENT_VERSION` | `3.1.3` | Agent/test-app version to fetch from Maven Central. |
| `MAVEN_REPO_URL` | `https://repo1.maven.org/maven2` | Maven repository base URL. |
| `SKIP_DOWNLOAD` | unset | Set to `1` to reuse a previously downloaded distribution instead of re-fetching it. |

This test is the acceptance gate for the fix: on the pre-fix backend it fails (zero calls — the connection never gets past the agent's first `INIT_STREAM_V2` for `gc`); after the fix it passes.
