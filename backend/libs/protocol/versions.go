package model

// Protocol versions exchanged in the GET_PROTOCOL_VERSION(_V2) handshake.
// Numerically identical to proto-definition/.../transport/ProtocolConst.java;
// see backend/docs/design/06-wire-protocol-server.md §3.
const (
	PROTOCOL_VERSION    uint64 = 100505 // legacy GET_PROTOCOL_VERSION reply
	PROTOCOL_VERSION_V2 uint64 = 100605 // handshake reply the collector must use
	PROTOCOL_VERSION_V3 uint64 = 100705 // version the agent offers; selects posDictionary if echoed back
	BLACK_LISTED_RESP   uint64 = 88888888
)

// Acknowledgement bytes the collector writes back to the agent (06 §5).
//
// A non-negative byte is the number of diagnostic commands the collector
// dispatches to the agent; the MVP never dispatches any, so it is always
// ACK_OK. ACK_ERROR_MAGIC is -1 as a signed byte (0xFF unsigned): it tells the
// agent the collector cannot accept data and forces a reconnect (06 §6).
const (
	ACK_OK          byte = 0x00
	ACK_ERROR_MAGIC byte = 0xFF
)

// DataBufferSize is the agent's DATA_BUFFER_SIZE (ProtocolConst.java): the
// fixed-size buffer a length-prefixed field is read into. The agent's
// FieldIOReader.Field() rejects any field whose length exceeds it, so the
// collector never receives a longer one; a length past this ceiling on the wire
// is a malformed or hostile client, and a decoder that honoured it would try to
// allocate up to 4 GiB from a single wire-supplied length. Both the server
// framing (libs/io) and the offline parser (libs/parser/pipe) cap fixed-string
// reads at this size (06 §2).
const DataBufferSize = 1024
