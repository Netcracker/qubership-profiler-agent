package model

type (
	StreamType = string
)

const (
	StreamDictionary = StreamType("dictionary")
	StreamParams     = StreamType("params")
	StreamSuspend    = StreamType("suspend")
	StreamCalls      = StreamType("calls")
	StreamTrace      = StreamType("trace")
	StreamSql        = StreamType("sql")
	StreamXml        = StreamType("xml")

	// StreamGc is not part of the MVP write contract (01-write-contract.md
	// §1 lists seven streams) — it carries no data the collector stores.
	// Agents built before v3.1.4 register it unconditionally whenever they
	// stream directly to a collector, regardless of whether GC-log
	// harvesting is even enabled (Dumper.java's gcOs is created and rotated
	// alongside every other output stream once remoteConfigured is true).
	// GC-log harvesting moved to diagtools in v3.1.4 (commit ac804ee3), so
	// the collector has nowhere to route these bytes; it accepts the stream
	// and discards its payload instead of refusing it, since refusing tears
	// down the whole pod-restart connection (06 §6), not just this stream.
	StreamGc = StreamType("gc")
)

var knownStreams = map[StreamType]struct{}{
	StreamDictionary: {},
	StreamParams:     {},
	StreamSuspend:    {},
	StreamCalls:      {},
	StreamTrace:      {},
	StreamSql:        {},
	StreamXml:        {},
	StreamGc:         {},
}

// IsKnownStream reports whether name is one of the eight streams the
// collector accepts. An INIT_STREAM_V2 for anything else is answered with a
// null handle and a teardown (06-wire-protocol-server.md §4, §6). Note: the
// agent's posDictionary stream is deliberately absent — the collector replies
// PROTOCOL_VERSION_V2 so the agent never opens it (06 §3). StreamGc is the
// odd one out among the eight: it is accepted for backward compatibility
// with agents built before v3.1.4, but its bytes are discarded, not stored
// (see the StreamGc doc comment).
func IsKnownStream(name StreamType) bool {
	_, ok := knownStreams[name]
	return ok
}
