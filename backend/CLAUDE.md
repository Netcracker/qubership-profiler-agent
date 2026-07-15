# Backend (`backend/`)

## Mandatory reading before any backend work

- `backend/docs/design/01-05` — contracts; treat as source of truth.
- `backend/docs/design/stage{N}-plan.md` — what's planned for the current stage.
- `backend/docs/design/stage{N}-progress.md` — what's done, decisions log, open issues. **Read before touching code; append after merging code.**
- `backend/docs/design/deferred.md` — ideas explicitly out of scope.

For implementation work (any change under `backend/apps/` or `backend/libs/`), also read `backend/docs/design/WORKFLOW.md` — branches, commits, PRs, tests, when to update which doc.

## Gotchas

**The word "chunk" means three different things.** Do not conflate them — the confusion produced a wrong recovery algorithm in the first Stage 0 draft:

- **`COMMAND_RCV_DATA` payload** — up to `DATA_BUFFER_SIZE` = 1 KB of one stream's bytes (`proto-definition/.../transport/ProtocolConst.java`). The agent chops every logical write at 1 KB (`dumper/.../client/DefaultCollectorClient.java`). The collector concatenates payloads per stream before anything else.
- **Logical trace chunk** — `[threadId, startTime]` (16 bytes) + events + `EVENT_FINISH_RECORD`, `LocalBuffer`-sized (tens of KB). One logical chunk spans many `RCV_DATA` payloads. It has no length prefix, so its boundary is found only by parsing events to `EVENT_FINISH_RECORD` (see `backend/libs/parser/pipe/traces.go`).
- **Go `Chunk` type** (`libs/protocol`) — a rolling-stream handle, unrelated to either of the above.

**Channel gzip is optional and off by default.** `ProtocolConst.ZIPPING_ENABLED = false`. When on, the *whole* multiplexed channel is one GZIP stream, so the collector must gunzip before it can demux `RCV_DATA`. The existing Go parser has no gunzip layer — it assumes an uncompressed dump.
