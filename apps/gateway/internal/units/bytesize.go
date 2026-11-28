// Package units holds the value types the gateway config uses beyond what
// the Collector's config decoder handles natively.
package units

import (
	"fmt"
	"strconv"
	"strings"
)

// ByteSize is a size in bytes. In config it is written as a plain integer,
// optionally followed by B or by one of the IEC suffixes KiB, MiB, GiB, and
// TiB (powers of 1024). KB, MB, GB, TB, and the one-letter K, M, G, T are
// rejected in any case: profiler-backend reads them as powers of 1024, and the
// SI convention reads them as powers of 1000.
type ByteSize int64

var binarySuffixes = map[string]int64{"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40}

// UnmarshalText parses a size such as "10GiB", "64MiB", or "4096".
func (b *ByteSize) UnmarshalText(text []byte) error {
	v, err := ParseByteSize(string(text))
	if err != nil {
		return err
	}
	*b = v
	return nil
}

// ParseByteSize parses a size such as "10GiB", "64MiB", or "4096".
func ParseByteSize(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	i := strings.IndexFunc(s, func(r rune) bool { return r < '0' || r > '9' })
	if i < 0 {
		i = len(s)
	}
	number, unit := s[:i], strings.TrimSpace(s[i:])
	factor, ok := binarySuffixes[unit]
	if !ok && unit != "" && unit != "B" {
		// "64M", "64mb", "64MB", and a miscased "64mib" get the IEC
		// spelling in the error.
		u := strings.ToUpper(unit)
		if number != "" && strings.ContainsAny(u[:1], "KMGT") {
			iec := number + u[:1] + "iB"
			switch u[1:] {
			case "", "B":
				return 0, fmt.Errorf("%q is ambiguous: write %s", s, iec)
			case "I", "IB":
				return 0, fmt.Errorf("invalid size %q: write %s", s, iec)
			}
		}
		number = "" // fails ParseInt below
	}
	if !ok {
		factor = 1
	}
	n, err := strconv.ParseInt(number, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q: use a non-negative integer with an optional unit such as KiB, MiB, or GiB", s)
	}
	if n > 0 && factor > 1 && n > (1<<63-1)/factor {
		return 0, fmt.Errorf("invalid size %q: overflows int64", s)
	}
	return ByteSize(n * factor), nil
}

// String formats the size in the largest binary unit that divides it.
func (b ByteSize) String() string {
	for _, u := range []struct {
		suffix string
		factor int64
	}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}} {
		if b != 0 && int64(b)%u.factor == 0 {
			return strconv.FormatInt(int64(b)/u.factor, 10) + u.suffix
		}
	}
	return strconv.FormatInt(int64(b), 10) + "B"
}
