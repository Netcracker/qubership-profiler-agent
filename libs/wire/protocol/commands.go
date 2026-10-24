package model

type (
	Command byte
)

// Command bytes, numerically identical to
// proto-definition/.../transport/ProtocolConst.java. The Java protocol has no
// 0x00 byte, so a leading 0x00 on the wire is not a command — the server treats
// it as unknown and tears the connection down (06 §2).
const (
	COMMAND_INIT_STREAM             Command = 0x01
	COMMAND_INIT_STREAM_V2          Command = 0x15
	COMMAND_RCV_DATA                Command = 0x02
	COMMAND_CLOSE                   Command = 0x04
	COMMAND_GET_PROTOCOL_VERSION    Command = 0x08
	COMMAND_GET_PROTOCOL_VERSION_V2 Command = 0x14
	COMMAND_REQUEST_ACK_FLUSH       Command = 0x11
	COMMAND_REPORT_COMMAND_RESULT   Command = 0x13
)
