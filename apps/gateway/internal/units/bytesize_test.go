package units

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseByteSize(t *testing.T) {
	for in, want := range map[string]ByteSize{
		"0":      0,
		"4096":   4096,
		"12B":    12,
		"32KiB":  32 << 10,
		"4MiB":   4 << 20,
		"10GiB":  10 << 30,
		"1TiB":   1 << 40,
		"64 MiB": 64 << 20,
		" 2GiB ": 2 << 30,
	} {
		got, err := ParseByteSize(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
}

func TestParseByteSizeRejects(t *testing.T) {
	for _, in := range []string{"", "MiB", "-1", "1.5GiB", "10 apples", "9999999999TiB", "5Kib", "1EiB"} {
		_, err := ParseByteSize(in)
		assert.Error(t, err, in)
	}
}

func TestByteSizeString(t *testing.T) {
	assert.Equal(t, "10GiB", ByteSize(10<<30).String())
	assert.Equal(t, "32KiB", ByteSize(32<<10).String())
	assert.Equal(t, "1000B", ByteSize(1000).String())
	assert.Equal(t, "0B", ByteSize(0).String())
}

func TestUnmarshalText(t *testing.T) {
	var b ByteSize
	require.NoError(t, b.UnmarshalText([]byte("16MiB")))
	assert.Equal(t, ByteSize(16<<20), b)
	assert.Error(t, b.UnmarshalText([]byte("x")))
}

func TestParseByteSizeRejectsDecimalSuffixes(t *testing.T) {
	for in, want := range map[string]string{
		"64MB":  `"64MB" is ambiguous: write 64MiB`,
		"64mb":  `"64mb" is ambiguous: write 64MiB`,
		"64M":   `"64M" is ambiguous: write 64MiB`,
		"5 kB":  `"5 kB" is ambiguous: write 5KiB`,
		"2GB":   `"2GB" is ambiguous: write 2GiB`,
		"1t":    `"1t" is ambiguous: write 1TiB`,
		"64mib": `invalid size "64mib": write 64MiB`,
		"64Mi":  `invalid size "64Mi": write 64MiB`,
	} {
		_, err := ParseByteSize(in)
		assert.EqualError(t, err, want, in)
	}
}
