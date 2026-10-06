package model

import (
	"math"
	"strings"
)

const (
	// ApiVersion schema version (for Parquet file, db schema, etc.)
	ApiVersion = 1
)

type DurationRange struct {
	Pos      int    // pos in list of durations
	From, To int    // start and end in ms
	Title    string // human readable
}

type DurationRanges struct {
	List []DurationRange
}

func DurationAsInt(dr *DurationRange) int {
	from := -1
	if dr != nil {
		from = (*dr).From
	}
	return from
}

func (dr DurationRanges) Get(duration int32) DurationRange {
	for _, r := range dr.List {
		if int(duration) < r.To {
			return r
		}
	}
	return dr.List[len(dr.List)-1]
}

func (dr DurationRanges) GetByName(duration string) *DurationRange {
	for _, r := range dr.List {
		if strings.ToLower(duration) == r.Title {
			return &r
		}
	}
	return nil
}

var (
	Durations = DurationRanges{
		[]DurationRange{
			{0, 0, 1, "0ms"},
			{1, 1, 10, "1ms"},
			{2, 10, 100, "10ms"},
			{3, 100, 1000, "100ms"},
			{4, 1000, 5000, "1s"},
			{5, 5000, 30000, "5s"},
			{6, 30000, 90000, "30s"},
			{7, 90000, math.MaxInt32, "90s"},
		},
	}
)
