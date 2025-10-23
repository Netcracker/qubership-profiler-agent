package utils

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

type (
	// Uuid simple representation for uuid structure
	// (can use during communication between k6 golang-module and javascript test scripts)
	Uuid struct {
		Val [16]byte
		Str string
	}
	LTime     = int64
	LDuration = int
	LCounter  = int
	LBytes    = uint64
)

func ToHex(val [16]byte) string {
	var bb strings.Builder
	for _, c := range val {
		fmt.Fprintf(&bb, "%02X:", c)
	}
	s := bb.String()
	return s[0 : len(s)-1]
}

func ToUuid(val [16]byte) Uuid {
	return Uuid{val, ToHex(val)}
}

func (u Uuid) ToBin() [16]byte {
	return u.Val
}

func (u Uuid) ToHex() string {
	return ToHex(u.Val)
}

func (u Uuid) String() string {
	return u.Str
}

func AsHex(val []byte, maxLen int) string {
	var bb strings.Builder
	for i, c := range val {
		if i > maxLen {
			fmt.Fprintf(&bb, "...")
			break
		}
		fmt.Fprintf(&bb, "%02X:", c)
	}
	s := bb.String()
	if len(s) == 0 {
		return ""
	}
	return s[0 : len(s)-1]
}

func ParseDate(date string) (time.Time, error) {
	location, err := time.LoadLocation("UTC")
	if err != nil {
		return time.Now(), fmt.Errorf("Error during loading of time-zone location %s", err.Error())
	}

	return time.ParseInLocation("2006/01/02", date, location)
}

func ParseHourTime(date string) (time.Time, error) {
	location, err := time.LoadLocation("UTC")
	if err != nil {
		return time.Now(), fmt.Errorf("Error during loading of time-zone location %s", err.Error())
	}

	return time.ParseInLocation("2006/01/02/15", date, location)
}

func DateHour(t time.Time) string {
	return t.Format("2006/01/02/15")
}

func Random(from, to int64) int64 {
	if from > to {
		from = 0
	}
	if from == to {
		return from
	}
	return rand.Int63n(to-from) + from
}

func RandomUuid() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}
	return AsHex(b, 30)
}

func RandomTime(hour time.Time) time.Time {
	return hour.Truncate(time.Hour).Add(time.Duration(rand.Int31n(60*60*1000)) * time.Millisecond)
}
