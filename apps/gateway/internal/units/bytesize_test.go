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
		"5KB":    5000,
		"64 MiB": 64 << 20,
		" 2GB ":  2e9,
	} {
		got, err := ParseByteSize(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
}

func TestParseByteSizeRejects(t *testing.T) {
	for _, in := range []string{"", "MiB", "-1", "1.5GiB", "10 apples", "9999999999TiB"} {
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
