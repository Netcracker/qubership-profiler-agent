# libs/wire

The profiler agent's wire protocol in Go: command and stream definitions, framing primitives, and the TCP server that
terminates an agent connection. The contract is in
[the wire protocol server design](../../docs/design/06-wire-protocol-server.md).

| Package | Contents |
|---|---|
| `protocol` | Command bytes, protocol versions, stream names, and the rolling-stream `Chunk` type. |
| `server` | The TCP listener, the per-connection command loop, and the `Listener` interface that receives the data. |
| `io` | Readers and writers for the protocol's fixed-width and length-prefixed fields. |
| `common` | Small helpers the other packages share: time, UUID, hex, and index types. |
| `log` | Context-scoped logging. |

## Why a separate module

`libs/wire` is its own Go module, `github.com/Netcracker/qubership-profiler-agent/libs/wire`, so that code outside this
repository can import the protocol. The repository's root module is named
`github.com/Netcracker/qubership-profiler-backend`. A different repository answers to that name, so an outside build
that imports a package under it fetches that repository's code, not this one's. A nested module with the correct path
avoids that. It also keeps the backend's storage dependencies out of the consumer's build.

Keep the module small. A package belongs here only if an outside consumer needs it, and it must not import anything
from the root module.

## Use inside this repository

The root `go.mod` requires the module at the placeholder version `v0.0.0` and replaces it with this directory:

```text
replace github.com/Netcracker/qubership-profiler-agent/libs/wire => ./libs/wire
```

The backend always builds against the code in the tree, so a protocol change and its callers go in one commit.

`go test ./...` from the repository root stops at the module boundary. Run this module's tests from here:

```bash
cd libs/wire && go test ./...
```

A Dockerfile that runs `go mod download` for the root module must copy `libs/wire/go.mod` and `libs/wire/go.sum` first.

## Releases

Outside consumers ignore the `replace` and fetch a tagged version. Go requires the tag of a nested module to carry the
directory prefix: `libs/wire/v0.1.0`, not `v0.1.0`. Cut the tag on `main` after the change merges.
