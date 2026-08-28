package pipe

import "io"

// ByteSender simple io.Reader for test purposes
type ByteSender struct {
	pos  int
	data []byte
}

func (bs *ByteSender) Read(p []byte) (n int, err error) {
	if bs.pos >= len(bs.data) {
		return 0, io.EOF
	}
	n = copy(p, bs.data[bs.pos:])
	bs.pos += n
	return n, nil
}

func NewByteSender(data []byte) io.Reader {
	return &ByteSender{0, data}
}
