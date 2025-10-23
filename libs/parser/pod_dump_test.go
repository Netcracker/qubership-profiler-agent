package parser

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestParsedPodDump_ByType(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	testDumpFile := filepath.Join(ResourceDir, "ui5min.protocol")
	data, err := ParsePodTcpDump(ctx, TcpFile{"ui5min.protocol", testDumpFile})
	assert.Nil(t, err)

	t.Run("parse", func(t *testing.T) {
		p := &ParsedPodDump{
			LoadedTcpData: data,
		}

		c := p.ParamsChunk()
		assert.NotNil(t, c)
		assert.Equal(t, "8A:43:7A:F2:58:74:47:73:8F:F2:CD:5B:83:FD:83:F8", c.Handle.Str)

		c = p.DictionaryChunk()
		assert.NotNil(t, c)
		assert.Equal(t, "95:D3:22:0F:CD:08:41:55:B3:D0:79:A0:06:D9:EB:C6", c.Handle.Str)

		c = p.LatestCallsChunk()
		assert.NotNil(t, c)
		assert.Equal(t, "A6:12:85:9D:A6:73:4C:90:B1:99:63:D0:8B:9A:DE:E4", c.Handle.Str)

		c = p.LatestTraceChunk()
		assert.NotNil(t, c)
		assert.Equal(t, "30:35:6C:4C:79:D5:41:DA:BB:8C:92:8D:01:6D:1E:A1", c.Handle.Str)

		c = p.LatestSqlChunk()
		assert.NotNil(t, c)
		assert.Equal(t, "69:7B:12:3B:31:7F:48:ED:BE:07:69:DD:99:92:9B:9D", c.Handle.Str)

		c = p.LatestXmlChunk()
		assert.NotNil(t, c)
		assert.Equal(t, "B5:D3:77:F2:A2:E0:41:ED:95:4B:DC:7C:5D:97:34:71", c.Handle.Str)

		c = p.ByType("suspend")
		assert.NotNil(t, c)
		assert.Equal(t, "71:B3:A9:A4:89:3C:4F:9A:A1:F5:00:5D:E7:BC:19:D5", c.Handle.Str)

		assert.Equal(t, "ops-profiler:esc-ui-service:esc-ui-service-8dd5b49fd-2gr2g_1691167327796", p.Name())

		p.ParseStreams(ctx, false, filepath.Join(ResourceDir, "generated"))
		//p.ParseStreams(ctx, true, filepath.Join(ResourceDir, "generated"))
	})
}
