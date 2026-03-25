package common

type (
	// UUID original binary data for uuid
	UUID = [16]byte

	// Uuid simple representation for uuid structure
	// (can use during communication between k6 golang-module and javascript test scripts, for example, profiler-protocol)
	Uuid struct {
		Val UUID
		Str string
	}
)

func ToUuid(val [16]byte) Uuid {
	return Uuid{val, ToHex(val)}
}

func (u Uuid) ToBin() UUID {
	return u.Val
}

func (u Uuid) ToHex() string {
	return ToHex(u.Val)
}

func (u Uuid) String() string {
	return u.Str
}
