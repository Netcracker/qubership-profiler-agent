package common

import (
	"fmt"
	"strings"
)

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

func ToHex(val [16]byte) string {
	var bb strings.Builder
	for _, c := range val {
		fmt.Fprintf(&bb, "%02X:", c)
	}
	s := bb.String()
	return s[0 : len(s)-1]
}
