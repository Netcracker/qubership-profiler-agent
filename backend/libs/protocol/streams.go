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
