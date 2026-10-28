// Package units holds the value types the gateway config uses beyond what
// the Collector's config decoder handles natively.
package units

import (
	"fmt"
	"strconv"
	"strings"
)

// ByteSize is a size in bytes. In config it is written as a plain integer or
// with a unit suffix: B, KB, MB, GB, TB (powers of 1000) or KiB, MiB, GiB,
// TiB (powers of 1024).
type ByteSize int64

var suffixes = []struct {
	suffix string
	factor int64
}{
	// Two-letter binary suffixes go first so that "MiB" is not read as "B".
	{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12},
	{"B", 1},
}

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
	factor := int64(1)
	number := s
	for _, u := range suffixes {
		if strings.HasSuffix(s, u.suffix) {
			factor = u.factor
			number = strings.TrimSpace(strings.TrimSuffix(s, u.suffix))
			break
		}
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
