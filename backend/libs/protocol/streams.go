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
)

var knownStreams = map[StreamType]struct{}{
	StreamDictionary: {},
	StreamParams:     {},
	StreamSuspend:    {},
	StreamCalls:      {},
	StreamTrace:      {},
	StreamSql:        {},
	StreamXml:        {},
}

// IsKnownStream reports whether name is one of the seven streams the collector
// serves. An INIT_STREAM_V2 for anything else is answered with a null handle
// and a teardown (06-wire-protocol-server.md §4, §6). Note: the agent's
// posDictionary stream is deliberately absent — the collector replies
// PROTOCOL_VERSION_V2 so the agent never opens it (06 §3).
func IsKnownStream(name StreamType) bool {
	_, ok := knownStreams[name]
	return ok
}
