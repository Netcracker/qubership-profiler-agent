package model

import (
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// Order is a /calls sort order (02 §2.3.5): `<field>_<direction>`. Every
// order breaks ties by PK ascending, so each one is a total order both tiers
// produce alike. The zero value is the default, start_desc.
type Order string

// The orders this build implements (02 §2.3.5, stage A).
const (
	OrderStartDesc    Order = "start_desc"
	OrderDurationDesc Order = "duration_desc"
)

// orderFields are the sort fields of the §2.3.5 table. An order on one of
// them that the build lacks is "not implemented", not unknown.
var orderFields = []string{"start", "duration", "cpu", "queue", "calls", "net"}

// ImplementedOrders lists the orders /calls accepts, for
// filter_capabilities.orders (02 §2.3.4).
var ImplementedOrders = []Order{OrderStartDesc, OrderDurationDesc}

// ParseOrder validates the `order` parameter. An empty value is the default.
// The error text is the 400 detail and names the order.
func ParseOrder(raw string) (Order, error) {
	if raw == "" {
		return OrderStartDesc, nil
	}
	field, dir, ok := strings.Cut(raw, "_")
	if !ok || !slices.Contains(orderFields, field) || (dir != "asc" && dir != "desc") {
		return "", errors.Errorf("unknown order %s: expected <field>_<asc|desc> with a field of %s",
			raw, strings.Join(orderFields, ", "))
	}
	if !slices.Contains(ImplementedOrders, Order(raw)) {
		return "", errors.Errorf("order %s is not implemented", raw)
	}
	return Order(raw), nil
}

// String returns the order's parameter value; the zero value reads as the
// default.
func (o Order) String() string {
	if o == "" {
		return string(OrderStartDesc)
	}
	return string(o)
}

// ByDuration reports whether the order sorts by duration_ms.
func (o Order) ByDuration() bool { return o == OrderDurationDesc }

func (o Order) key(p Position) int64 {
	if o.ByDuration() {
		return int64(p.DurationMs)
	}
	return p.TsMs
}

// Before reports whether a sorts strictly before b: the higher sort key
// first, then the lower PK. A position with no PK sorts after every row of
// its key, so it passes only strictly lower keys.
func (o Order) Before(a, b Position) bool {
	if ka, kb := o.key(a), o.key(b); ka != kb {
		return ka > kb
	}
	switch {
	case a.NoPK:
		return false
	case b.NoPK:
		return true
	}
	return a.PK.Compare(b.PK) < 0
}

// Past reports whether p sits strictly past the cursor position: the keyset
// seek predicate of 02 §2.3.1 for this order.
func (o Order) Past(p, cursor Position) bool {
	return o.Before(cursor, p)
}

// SortRows sorts rows into the order, so an unsorted batch becomes a merge
// run.
func (o Order) SortRows(rows []CallRow) {
	slices.SortFunc(rows, func(a, b CallRow) int {
		ap, bp := a.Position(), b.Position()
		switch {
		case o.Before(ap, bp):
			return -1
		case o.Before(bp, ap):
			return 1
		}
		return int(a.Tier) - int(b.Tier)
	})
}

// DurationBand is one clean retention class read as a duration band by
// duration_desc (02 §2.3.5): it holds clean calls with LowerMs <= duration_ms
// < UpperMs.
type DurationBand struct {
	Class   string
	LowerMs int64
	UpperMs int64
}

// DurationBands derives the clean bands from the tier table and the
// classification thresholds, longest band first. nil thresholds select the
// table defaults.
func DurationBands(thresholds []time.Duration) []DurationBand {
	upper := CleanClassUpperMs(thresholds)
	clean := CleanTiers()
	bands := make([]DurationBand, len(clean))
	lower := int64(0)
	for i, t := range clean {
		bands[len(clean)-1-i] = DurationBand{Class: t.Class, LowerMs: lower, UpperMs: upper[t.Class]}
		lower = upper[t.Class]
	}
	return bands
}

// BandFor returns the class of the longest band a page past the cursor
// position may still need: the band that holds the position's duration, or,
// for a position with no PK, the band just below it.
func BandFor(bands []DurationBand, cursor Position) string {
	d := int64(cursor.DurationMs)
	if cursor.NoPK {
		d--
	}
	for _, b := range bands {
		if d >= b.LowerMs {
			return b.Class
		}
	}
	return bands[len(bands)-1].Class
}
