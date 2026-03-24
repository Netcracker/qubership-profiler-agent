package inventory

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestServices(t *testing.T) {

	t.Run("add service", func(t *testing.T) {
		s := Services{set: make(map[string]bool)}

		s.AddMap(map[string]any{"svc01": true, "svc20": true, "svc05": true})
		assert.Equal(t, 3, s.Size())
		assert.Equal(t, []string{"svc01", "svc05", "svc20"}, s.List())

		s.AddList([]string{"svc06", "svc10", "svc03"})
		assert.Equal(t, 6, s.Size())
		assert.Equal(t, []string{"svc01", "svc03", "svc05", "svc06", "svc10", "svc20"}, s.List())
	})
}
