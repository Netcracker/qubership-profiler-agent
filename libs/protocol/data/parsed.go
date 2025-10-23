package data

import (
	"sort"
	"time"
)

type (
	Param struct {
		Pos       int
		Bytes     int
		Name      string
		IsIndex   bool
		IsList    bool
		Order     int
		Signature string
	}
	Params struct {
		List []Param
	}

	DictWord struct {
		Pos   int
		Bytes int
		Word  string
	}
	Dictionary struct {
		List []DictWord
	}

	Suspend struct {
		Pos    int
		Bytes  int
		Time   time.Time
		Delta  int
		Amount int
	}
	Suspends struct {
		List      []Suspend
		StartTime time.Time
		EndTime   time.Time
	}

	TraceRecord struct {
		Pos   int
		Bytes int
		Time  time.Time
		Data  []byte
	}
	Traces struct {
		List []TraceRecord
	}

	CallInfo struct {
		Pos   int
		Bytes int
		Time  time.Time
		Call  Call
	}
	Calls struct {
		List        []*CallInfo
		RequiredIds map[TagId]bool
	}
)

func (d Dictionary) Get(i int) string {
	if i >= 0 && i < len(d.List) {
		return d.List[i].Word
	}
	return "?"
}

func (c Calls) Tags() []TagId {
	list := make([]TagId, 0, len(c.RequiredIds))
	for k, _ := range c.RequiredIds {
		list = append(list, k)
	}
	sort.Ints(list)
	return list
}
