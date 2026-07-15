# 06 — Wire protocol, server side

> Status: **draft**, awaiting review. Verified against agent code (`dumper/`, `proto-definition/`) and the Go server (`backend/libs/server/`). The server implementation now conforms (§8), guarded by an integration test (§9). No agent change is required.

Contract `01-write-contract.md` §1 covers what the agent **sends** over the TCP channel — the seven named streams and the framing of each. This document covers the other half of the same socket: what the collector **reads from each command and writes back**, on which events it flushes, and how it acknowledges data. It is the source of truth for the TCP listener in Stage 1.1.

The distinction matters because the response direction is not a parser concern. `backend/libs/parser/parser.go` is an offline dump reader: for every command it reads *both* the request fields *and* the server's reply from the same input stream (for example `svrProtocol` at `parser.go:126-130`, the `INIT_STREAM_V2` handle and rotation fields at `parser.go:178-198`). On a live socket those reads do not exist — the collector is the party that produces those bytes. The response state machine is new code, not a parser extension.

A live server already exists (`backend/libs/server/server_connection.go`). It was a skeleton that diverged from this contract in several places; §8 records those divergences and the fixes that brought it into conformance.

## 1. Model

- **One TCP connection, one agent, one `(namespace, service, podName)` triple** (`01-write-contract.md` §1 V6). The collector accepts the connection and stamps `restartTime` at accept time (§1 V4).
- **Request/response over a single duplex socket.** The agent drives: it sends a command, and for the commands that expect a reply it reads the reply before sending the next data command. The collector never initiates a command in the MVP; it only answers.
- **Framing primitives** are shared with the read path and already exist on the Go side (`backend/libs/io/tcp_writer.go`): `WriteFixedByte`, `WriteFixedInt`, `WriteFixedLong`, `WriteUuid`, `WriteFixedString`, `WriteFixedBuf`. Field encodings match the agent's `FieldIO` (`proto-definition/.../transport/`). "Fixed" long/int are big-endian; strings and byte fields are length-prefixed.
- **Command bytes** are defined once in `backend/libs/protocol/commands.go` and must stay numerically identical to `proto-definition/.../transport/ProtocolConst.java`.

## 2. Command table

Every command the agent can send, what the collector reads, what it writes back, and when it flushes. `→` marks bytes the collector writes to the socket. Source: `backend/apps/collector/.../ProfilerAgentReader.java:82-218` (the reference Java implementation) and `DefaultCollectorClient.java` (the agent side).

| Command | Byte | Request fields (read) | Response (write) | Flush |
|---|---|---|---|---|
| `GET_PROTOCOL_VERSION_V2` | `0x14` | `long` clientVersion, `string` pod, `string` service, `string` namespace | → `long` `PROTOCOL_VERSION_V2` (§3) | force |
| `INIT_STREAM_V2` | `0x15` | `string` streamName, `int` requestedRollingSeq, `int` resetRequired | → `UUID` handle, `long` rotationPeriod, `long` requiredRotationSize, `int` serverRollingSeq (§4); on unknown stream → `UUID` null then close (§6) | force |
| `RCV_DATA` | `0x02` | `UUID` handle, field (≤ 1 KB payload) | → one ack byte (§5); on error → `ACK_ERROR_MAGIC` then close (§6) | ≤ 500 ms |
| `REQUEST_ACK_FLUSH` | `0x11` | — | → one ack byte, forced flush (§5) | force |
| `REPORT_COMMAND_RESULT` | `0x13` | `UUID` commandId, `byte` success | — (no reply) | — |
| `CLOSE` | `0x04` | — | — (close connection) | — |
| `GET_PROTOCOL_VERSION` | `0x08` | — | → `long` `PROTOCOL_VERSION` (legacy; §3) | force |
| `INIT_STREAM` | `0x01` | `string` namespace, `string` service, `string` pod, then falls through to `INIT_STREAM_V2` | as `INIT_STREAM_V2` | force |

**MVP scope.** Current agents open with `GET_PROTOCOL_VERSION_V2` and then use `INIT_STREAM_V2` / `RCV_DATA` / `REQUEST_ACK_FLUSH` / `CLOSE` (`DefaultCollectorClient.java:134`, `attemptCreateRollingChunk`, `attemptWrite`, `requestAckFlush`, `close`). The deprecated `0x01` and `0x08` are not emitted by any agent the MVP targets; the collector may reject them with `ACK_ERROR_MAGIC` + close rather than implement them, but the byte values are reserved and must not be reused.

The collector must reject any other command byte: log it, write `ACK_ERROR_MAGIC`, and close (§6). A silent skip corrupts stream framing, because the next byte is a field of the unknown command, not a new command.

## 3. Handshake — respond `PROTOCOL_VERSION_V2`, never `V3`

The agent opens every connection with `GET_PROTOCOL_VERSION_V2`, sending its own version `PROTOCOL_VERSION_V3` = `100705` (`DefaultCollectorClient.java:134-139`). The reply selects the dictionary wire format for the rest of the connection:

- Reply `PROTOCOL_VERSION_V2` = `100605` → the agent sends the `dictionary` stream: each record is `[len][utf-8 string]`, ids implied by arrival order (`Dumper.java:1266-1268`).
- Reply `PROTOCOL_VERSION_V3` = `100705` → the agent switches to the `posDictionary` stream: each record is `[varint id][string]` (`Dumper.java:350-354, 1269-1273`).

**The collector MUST reply `PROTOCOL_VERSION_V2`.** The redesign's stream set (`01-write-contract.md` §1) and the Go parser know `dictionary`, not `posDictionary` (`backend/libs/protocol/streams.go` lists seven streams, none of them `posDictionary`). Replying `V3` silently switches the agent to a stream the collector cannot demux — the dictionary is lost and every trace byte that references it becomes undecodable. This is a data-loss bug with no error surfaced on either side, which is exactly why it belongs in a contract.

The agent accepts either `V2` or `V3` as a successful handshake (`DefaultCollectorClient.java:142`), so replying `V2` while the agent offered `V3` is a normal, supported downgrade — the agent already carries the `V2` dictionary path for it.

The agent's version check is strict: any reply other than `V2`, `V3`, or `BLACK_LISTED_RESP` (`88888888`) makes it throw `ProfilerProtocolException` and drop the socket (`DefaultCollectorClient.java:142-164`). There is no renegotiation.

`GET_PROTOCOL_VERSION` (`0x08`, legacy) replies with the older `PROTOCOL_VERSION` = `100505`. Blacklisting (reply `BLACK_LISTED_RESP` to refuse a namespace) is out of MVP scope.

## 4. `INIT_STREAM_V2` response

The agent opens one stream file with `INIT_STREAM_V2`, sending `streamName`, `requestedRollingSequenceId`, and `resetRequired` (`attemptCreateRollingChunk`, `DefaultCollectorClient.java:271-299`). The collector replies with four fields, in order:

| Field | Type | Meaning | MVP value |
|---|---|---|---|
| handle | `UUID` | Opaque stream handle. The agent stores it and sends it in every `RCV_DATA` for this stream (`streamHandles.put`, `write`). | A fresh non-nil UUID, unique per open stream, stable for the stream's lifetime. |
| rotationPeriod | `long` | Wall-clock rotation hint; the agent opens a new stream file when it elapses. | From config; governs time-based file rotation on the agent. |
| requiredRotationSize | `long` | Byte size at which the agent rotates the stream file, opening the next `rollingSequenceId` via a new `INIT_STREAM_V2`. | `PROFILER_SEGMENT_ROTATION_SIZE`, default 4 MB (`01-write-contract.md` §4.4, §9). |
| serverRollingSequenceId | `int` | The sequence id the collector wants this stream file addressed by. | Echo the agent's `requestedRollingSequenceId`; on `resetRequired = 1` the stream restarts from the agent's requested id. |

The handle must be non-nil and stable: the agent keys every subsequent `RCV_DATA` by it, so a zero or changing handle desynchronizes stream routing. `requiredRotationSize` is how the collector keeps its PV segments 1:1 with agent stream files — the agent, not the collector, splits the stream, so this value is the collector's only lever over segment size (`01-write-contract.md` §4.4, decision 2026-07-01 in `stage0-progress.md`).

An unknown or unregisterable `streamName` gets a null-UUID reply followed by a close (§6), mirroring `ProfilerAgentReader.java:104-110`.

## 5. Acknowledgement policy

The agent tracks one pending ack per `RCV_DATA` it sends (`pendingAcks++` in `attemptWrite`, `DefaultCollectorClient.java:335`) and per `REQUEST_ACK_FLUSH`. It does not block on each write; instead the dumper flushes on a 5 s cadence (`MAX_FLUSH_INTERVAL_MILLIS`) and, at flush, drains every pending ack synchronously under a 30 s socket read timeout (`validateWriteDataAcks(true)`, `DefaultCollectorClient.java:344-352`; `PLAIN_SOCKET_READ_TIMEOUT = 30000`, `ProtocolConst.java:10`).

**Rule: the collector writes exactly one ack byte per `RCV_DATA` and per `REQUEST_ACK_FLUSH`.** The byte's value is the count of diagnostic commands the collector is dispatching back to the agent (`sendCommands`, `ProfilerAgentReader.java:239-244`):

- **`0`** — no pending command. **This is the only value the MVP ever writes.** Diagnostic commands (heap / thread / top dumps) are out of scope until Stage C5 (`01-write-contract.md` §10), so the collector never dispatches any.
- **`n > 0`** — the collector has `n` diagnostic commands queued; after the byte it must write `n × (UUID commandId, string command)` descriptors, which the agent reads and executes (`validateAckSync` → `dispatchCommands`, `DefaultCollectorClient.java:417-431`). Not used in the MVP.
- **`ACK_ERROR_MAGIC` = `-1`** — a fatal signal: the agent throws and forces a reconnect (§6). Reserved for the error path; never a normal ack.

**Flush timing.** The ack byte for `RCV_DATA` is written unflushed and flushed on the collector's own cadence — at latest on the next `REQUEST_ACK_FLUSH` (forced flush) or the periodic flush check (`FLUSH_CHECK_INTERVAL_MILLIS = 500`). Any cadence well under the agent's 30 s ack-read timeout is correct; the reference server flushes within 500 ms. `REQUEST_ACK_FLUSH` itself always forces a flush so the agent's synchronous drain returns promptly.

**Failure mode if the ack is missing.** If the collector reads `RCV_DATA` but never writes the byte, the agent's `pendingAcks` grows without bound; the 5 s flush then blocks in `validateAckSync` until the 30 s socket timeout fires, throws, and reconnects — a silent throughput collapse that looks like a network fault. The synthetic test (§9) exists to catch exactly this.

## 6. Error handling and connection teardown

The collector signals an unrecoverable condition with `ACK_ERROR_MAGIC` = `-1` and then closes the socket. The agent treats `ACK_ERROR_MAGIC` as "collector cannot accept data, rotation requested", throws, and reconnects from a clean state (`validateAckSync`, `DefaultCollectorClient.java:424-426`; reconnect resets the dictionary, `01-write-contract.md` §3.7). Cases:

| Condition | Response | Reference |
|---|---|---|
| `RCV_DATA` for an unregistered handle | `ACK_ERROR_MAGIC`, close | `ProfilerAgentReader.java:147-150` |
| `RCV_DATA` handler throws (write error, disk, decode) | `ACK_ERROR_MAGIC`, close | `ProfilerAgentReader.java:151-155` |
| `INIT_STREAM_V2` for an unknown `streamName` | null-UUID reply, close | `ProfilerAgentReader.java:104-110` |
| `INIT_STREAM_V2` handler throws | null-UUID reply, close | `ProfilerAgentReader.java:127-132` |
| Unknown command byte | `ACK_ERROR_MAGIC`, close | new — the reference Java throws (`ProfilerAgentReader.java:211-212`); the collector should signal before closing so the agent reconnects rather than stalling |
| EOF (`read()` returns `-1`) | close | `ProfilerAgentReader.java:89-90` |
| `CLOSE` (`0x04`) | close, no reply | `ProfilerAgentReader.java:165-167` |

Reconnect is always a fresh pod-restart on the collector side: a new TCP accept, a new `restartTime`, a full dictionary re-sent by the agent with `resetRequired = 1` (`01-write-contract.md` §3.7). The collector therefore never needs to preserve per-connection state across a drop.

## 7. Channel gzip

`ProtocolConst.ZIPPING_ENABLED` is `false` by default (`ProtocolConst.java:46`). When on, the whole multiplexed channel — commands, fields, and payloads — is one GZIP stream, so the collector must gunzip the socket before it can read a single command byte, and gzip its replies symmetrically (`DefaultCollectorClient.java:122-128`). The MVP targets the default (off); a gunzip/gzip wrapper around the socket is the only change needed if a deployment turns it on. Cross-reference: `01-write-contract.md` §1 and `backend/CLAUDE.md`.

## 8. Conformance of the `libs/server` implementation

The live server (`backend/libs/server/`) was a skeleton that predated this contract and diverged from §2–§6 in five ways. All five are now fixed; the list is the conformance record and the regression surface for §9.

1. **Handshake version** — was `ProtocolVersion = 10`, which a real agent rejects, dropping the socket (§3). Now `ProtocolVersion = PROTOCOL_VERSION_V2` (`libs/server/common.go`), with the version and ack constants defined once in `libs/protocol/versions.go`.
2. **`RCV_DATA` ack** — the ack write was commented out, so a real agent's flush cycle stalled into a reconnect (§5). `CommandRcvData` now writes one `ACK_OK` byte per payload, and `REQUEST_ACK_FLUSH` writes one `ACK_OK` and forces the flush that drains them.
3. **`INIT_STREAM_V2` reply** — the four fields were all zero. The server now assigns a fresh non-nil `RandomUuid` handle, echoes the requested rolling sequence, and returns `RotationPeriod` / `RequiredRotationSize` from `ConnectionOpts` (defaulting to 4 MB) (§4).
4. **Unknown stream** — `CommandInitStream` now validates `streamType` with `model.IsKnownStream` and replies a null UUID before tearing the connection down (§6).
5. **Unknown command** — the default branch now writes `ACK_ERROR_MAGIC` before erroring, so the agent reconnects instead of stalling; `COMMAND_CLOSE` ends the loop and the handler closes the socket on exit (§6).

The Go emulator still does not police the handshake reply on its own, so the integration test asserts the version explicitly (§9).

## 9. Synthetic test

Validation is a synthetic integration test, not golden output (`profiler-plan.md`). `backend/libs/tests/integration/emulator_test.go` drives the emulator against the live server and asserts:

1. **Handshake version.** `InitializeConnection` offers `PROTOCOL_VERSION_V3`; the test asserts `ServerVersion()` is `PROTOCOL_VERSION_V2` (§3).
2. **Flush cycle without reconnect.** `INIT_STREAM_V2` → several `RCV_DATA` → flush → `WaitForAcks` drains every ack with no `ACK_ERROR_MAGIC` and no timeout (§5). This is the regression guard for the §8.2 ack bug.
3. **Unknown stream refused.** `INIT_STREAM_V2` with a bogus stream name yields no valid handle and tears the connection down (§6).

Stronger checks left open: driving the real `Dumper` instead of the emulator, and asserting the dictionary decodes as `[len][string]` on the `dictionary` stream (the observable proof of the `V2` reply).

## 10. Review checklist

Before this document is merged and Stage 1.1 starts, please confirm or correct:

- [ ] Handshake reply fixed at `PROTOCOL_VERSION_V2`; `V3` explicitly forbidden (§3).
- [ ] Ack policy — one byte per `RCV_DATA` / `REQUEST_ACK_FLUSH`, value `0` in MVP, no diagnostic-command dispatch (§5).
- [ ] `INIT_STREAM_V2` response semantics, especially non-nil stable handle and `requiredRotationSize` as the segment-size lever (§4).
- [ ] Unknown-stream and unknown-command behaviour — `ACK_ERROR_MAGIC` / null-UUID + close (§6).
- [ ] Deprecated `0x01` / `0x08` — reserved but not implemented; reject with close (§2).
- [x] `libs/server` brought into conformance (§8), with a regression test (§9).
