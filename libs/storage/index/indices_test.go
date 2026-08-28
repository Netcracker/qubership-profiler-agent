package index

import (
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/stretchr/testify/assert"
)

var (
	empty = map[string]bool{}
	one   = map[string]bool{"test": true}
	tags  = map[string]bool{"param1": true, "param2": true, "param3": true, "param4": true}
)

func TestMap_Add(t *testing.T) {
	t.Run("empty tags", func(t *testing.T) {
		im := NewMap(empty)
		assert.Equal(t, 0, im.ParametersCount())
		im.addParameter("uid1", "param1", "value")
		assert.Equal(t, 0, im.ParametersCount())
	})

	t.Run("one tag", func(t *testing.T) {
		im := NewMap(one)
		assert.Equal(t, 0, im.ParametersCount())
		im.addParameter("uid1", "param1", "value")
		assert.Equal(t, 0, im.ParametersCount())
		im.addParameter("uid1", "test", "value")
		assert.Equal(t, 1, im.ParametersCount())
		assert.Equal(t, []string{"test"}, im.Parameters())
	})

	t.Run("several tags", func(t *testing.T) {
		im := NewMap(tags)
		assert.Equal(t, 0, im.ParametersCount())

		im.addParameter("uid1", "param1", "value")
		assert.Equal(t, 1, im.ParametersCount())
		assert.Equal(t, []string{"param1"}, im.Parameters())

		im.addParameter("uid1", "param4", "value")
		assert.Equal(t, 2, im.ParametersCount())
		assert.Equal(t, []string{"param1", "param4"}, im.Parameters())

		im.addParameter("uid1", "param3", "value")
		assert.Equal(t, 3, im.ParametersCount())
		assert.Equal(t, []string{"param1", "param3", "param4"}, im.Parameters())
	})

	t.Run("add map", func(t *testing.T) {
		im := NewMap(tags)
		assert.Equal(t, 0, im.ParametersCount())

		uuid1 := common.ToUuid(common.UUID{1: 5})

		paramValues := map[string][]string{
			"param1": {"val1", "val2"},
			"param4": {"val3"},
			"bad":    {"ads"},
		}

		for k, values := range paramValues {
			im.AddValues(uuid1, k, values)
		}
		assert.Equal(t, 2, im.ParametersCount())
		assert.Equal(t, []string{"param1", "param4"}, im.Parameters())

		assert.Equal(t, "IndexMap{map["+
			"param1:["+
			"IdxVal{Value: val1, FileId: 00:05:00:00:00:00:00:00:00:00:00:00:00:00:00:00}\n "+
			"IdxVal{Value: val2, FileId: 00:05:00:00:00:00:00:00:00:00:00:00:00:00:00:00}\n] "+
			"param4:[IdxVal{Value: val3, FileId: 00:05:00:00:00:00:00:00:00:00:00:00:00:00:00:00}\n]]}", im.String())
	})

}
