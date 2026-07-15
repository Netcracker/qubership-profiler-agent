# Real-agent E2E harness

Drives the actual Java profiler agent (built from this repo) against the Go backend and asserts that adversarial method and parameter strings round-trip byte-exact. This is the acceptance gate for the decoder fixes.

The test **fails today** on two backend decoder bugs. That failing assertion is the deliverable — do not change decoder code to make it pass.

## What it exercises

The existing Go smoke test (`backend/libs/tests/smoke`) feeds the collector with bytes from a Go emulator. The emulator's own encoder mirrors the buggy decoder, so it cannot surface these bugs. This harness sends bytes from the **real agent**, whose `DataOutputStreamEx.writeChars` writes faithful UTF-16 — which is what exposes them.

- **Bug A — `readChar` signedness** (`backend/libs/parser/pipe/pipe_reader.go`). The reader reads a signed `int16`, so every UTF-16 code unit `>= U+8000` (most CJK and Hangul) and both halves of a non-BMP surrogate pair (emoji) decode to `U+FFFD`. It corrupts every string: method names, class names, parameter keys, parameter values, thread names.
- **Bug B — empty dictionary word** (`backend/libs/parser/pipe/dictionary.go`). The reader skips an empty word without advancing its id counter, and the collector appends words by arrival order, so every later id shifts down by one and resolves to the wrong method or parameter name.

## Moving parts

| File | Role |
| --- | --- |
| `test-app/src/main/java/com/netcracker/profilerTest/testapp/AdversarialMain.java` | The workload. Records two synthetic calls through the `Profiler` API: Call A carries adversarial Unicode (bug A); Call B resolves an empty dictionary word first, then plain-ASCII names (bug B). |
| `scripts/e2e-realagent/config/_config.xml` | Profiler config. Marks the test-app package `do-not-profile` so the only recorded calls are the two synthetic ones. |
| `scripts/e2e-realagent/run-agent.sh` | Builds the agent and the test-app jar, then runs the workload under `-javaagent` pointed at the collector. |
| `backend/libs/tests/smoke_realagent/realagent_test.go` | The `//go:build smoke_realagent` test. Polls `/api/v1/calls`, fetches each call's `/tree`, and asserts the strings byte-exact. |

## Prerequisites

- A JDK 17+ and the repo's Gradle build (the harness builds the agent).
- Docker, for the backend `docker-compose` stack.

## Run it

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

The Go test shells out to `run-agent.sh`, so it needs the JDK and Gradle on `PATH`.

### Run the data-producing half alone

Useful for eyeballing what the backend stores. With the stack already up:

```bash
scripts/e2e-realagent/run-agent.sh
```

Environment knobs (all optional):

| Variable | Default | Meaning |
| --- | --- | --- |
| `COLLECTOR_HOST` | `localhost` | Collector TCP host. |
| `COLLECTOR_PORT` | `1715` | Collector TCP port. |
| `CLOUD_NAMESPACE` | `e2e-realagent` | Pod namespace the agent reports. |
| `MICROSERVICE_NAME` | `adversarial-app` | Service the agent reports. |
| `SKIP_BUILD` | unset | Set to `1` to reuse an already-built agent and jar. |

## How the agent reaches the collector

The agent switches from local-file dumps to the TCP collector purely because `REMOTE_DUMP_HOST` is set. From `dumper/.../Dumper.java`:

```
remoteConfigured  = isNotEmpty(REMOTE_DUMP_HOST)
localDumpEnabled  = forceLocalDump || !remoteConfigured
```

The plain port defaults to `ProtocolConst.PLAIN_SOCKET_PORT` (1715) when `REMOTE_DUMP_PORT_SSL` is unset; the harness also passes `REMOTE_DUMP_PORT_PLAIN` for clarity. Namespace and service come from `CLOUD_NAMESPACE` / `MICROSERVICE_NAME` (`VariableFinder`); the pod name is the host name plus a timestamp, so the test matches calls by namespace and service rather than by pod name.

Both keys are read as either JVM system properties (`-D…`) or environment variables (`PropertyFacadeBoot.getPropertyOrEnvVariable`). The harness passes them as `-D` flags.

The agent also needs its plugin jars. It auto-detects them from `${profiler.home}/lib`, so the harness sets `-Dprofiler.home` to the extracted installer home while keeping `-Dprofiler.config` on the in-tree adversarial config.

## Expected failure

Six assertions fail, three per bug.

Bug A — Call A:

```
method:      void com.acme.Svc.語한🔥_handle() ...   ->   void com.acme.Svc.����_handle() ...
param key:   param.語한🔥                             ->   param.����
param value: value-語한🔥-tail                        ->   value-����-tail
```

Bug B — Call B, where a single empty word shifts every later id by one:

```
method:      void com.acme.Svc.plainAsciiHandleB() ...   ->   param.b.alpha
param.b.alpha -> value-b-alpha                            ->   value-b-alpha bound under param.b.beta
param.b.beta  -> value-b-beta                             ->   value-b-alpha
```

## What a fix needs to touch

- Bug A: `backend/libs/parser/pipe/pipe_reader.go` — `readChar` must read an unsigned 16-bit code unit and reassemble surrogate pairs into the full code point, matching the Java reference (`DictionaryPhraseReader` / `DataInputStreamEx.readString`).
- Bug B: `backend/libs/parser/pipe/dictionary.go` — `DictionaryPipeReader` must register every word, including the empty string, so ids stay aligned with the agent (`DictionaryPhraseReader` registers every entry).
