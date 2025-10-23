package data

import (
	"fmt"
)

type (
	Dump struct {
		Id          int
		Namespace   string
		ServiceName string
		PodName     string
		DumpType    string
		Time        int64
		Duration    int32
		BytesSize   int32
		ThreadCount int32
		BinaryData  string
	}
)

func (d *Dump) String() string {
	return fmt.Sprintf("%+v", *d)
}
