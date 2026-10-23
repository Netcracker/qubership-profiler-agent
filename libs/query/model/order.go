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

// The orders that read the retention classes as duration bands, and the
// default.
const (
	OrderStartDesc    Order = "start_desc"
	OrderDurationDesc Order = "duration_desc"
	OrderDurationAsc  Order = "duration_asc"
)

// The sort fields of the §2.3.5 table.
const (
	OrderFieldStart    = "start"
	OrderFieldDuration = "duration"
	OrderFieldCPU      = "cpu"
	OrderFieldQueue    = "queue"
	OrderFieldCalls    = "calls"
	OrderFieldNet      = "net"
)

var orderFields = []string{
	OrderFieldStart, OrderFieldDuration, OrderFieldCPU, OrderFieldQueue, OrderFieldCalls, OrderFieldNet,
}

// ImplementedOrders lists the orders /calls accepts, for
// filter_capabilities.orders (02 §2.3.4): every field in both directions.
var ImplementedOrders = func() []Order {
	out := make([]Order, 0, 2*len(orderFields))
	for _, f := range orderFields {
		out = append(out, Order(f+"_desc"), Order(f+"_asc"))
	}
	return out
}()

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

// Field returns the sort field: start, duration, cpu, queue, calls, or net.
func (o Order) Field() string {
	field, _, _ := strings.Cut(o.String(), "_")
	return field
}

// Desc reports whether the order puts the highest sort key first.
func (o Order) Desc() bool { return strings.HasSuffix(o.String(), "_desc") }

// ByDuration reports whether the order sorts by duration_ms, in either
// direction: such an order walks the duration bands (02 §2.3.5).
func (o Order) ByDuration() bool { return o.Field() == OrderFieldDuration }

// NeedsSort reports whether rows in the order the stores keep them, start_desc
// within a file or a partition page, must be sorted into this order.
func (o Order) NeedsSort() bool { return o.String() != string(OrderStartDesc) }

// Key returns the position's sort key under the order.
func (o Order) Key(p Position) int64 {
	switch o.Field() {
	case OrderFieldDuration:
		return int64(p.DurationMs)
	case OrderFieldCPU:
		return p.CpuTimeMs
	case OrderFieldQueue:
		return int64(p.QueueWaitMs)
	case OrderFieldCalls:
		return int64(p.ChildCalls)
	case OrderFieldNet:
		return p.NetBytes
	}
	return p.TsMs
}

// PositionAt returns a position with the given sort key and no PK; the
// caller sets the PK of a row position.
func (o Order) PositionAt(key int64) Position {
	p := Position{NoPK: true}
	switch o.Field() {
	case OrderFieldDuration:
		p.DurationMs = int32(key)
	case OrderFieldCPU:
		p.CpuTimeMs = key
	case OrderFieldQueue:
		p.QueueWaitMs = int32(key)
	case OrderFieldCalls:
		p.ChildCalls = int32(key)
	case OrderFieldNet:
		p.NetBytes = key
	default:
		p.TsMs = key
	}
	return p
}

// Before reports whether a sorts strictly before b: the higher sort key
// first for a descending order and the lower one for an ascending order,
// then the lower PK. A position with no PK sorts after every row of its key,
// so it passes only keys strictly later in the order.
func (o Order) Before(a, b Position) bool {
	if ka, kb := o.Key(a), o.Key(b); ka != kb {
		return (ka > kb) == o.Desc()
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

// WalkBands returns the bands in the order a duration order reads them
// (02 §2.3.5): longest first for duration_desc, shortest first for
// duration_asc. bands come longest first, as DurationBands returns them.
func (o Order) WalkBands(bands []DurationBand) []DurationBand {
	out := slices.Clone(bands)
	if !o.Desc() {
		slices.Reverse(out)
	}
	return out
}

// Frontier returns the key that bounds the band in the walk direction: every
// row at or before it in the order belongs to this band or an earlier one.
// It is the lower bound of the band for duration_desc and the last duration
// inside it for duration_asc.
func (o Order) Frontier(b DurationBand) int64 {
	if o.Desc() {
		return b.LowerMs
	}
	return b.UpperMs - 1
}

// Reaches reports whether a duration sits at or before the frontier in the
// order.
func (o Order) Reaches(durationMs, frontier int64) bool {
	if o.Desc() {
		return durationMs >= frontier
	}
	return durationMs <= frontier
}

// BandFor returns the class of the first band in the walk direction that a
// page past the cursor position may still need: the band that holds the
// position's duration, or, for a position with no PK, the next band when the
// position ends its band.
func BandFor(o Order, bands []DurationBand, cursor Position) string {
	d := int64(cursor.DurationMs)
	if cursor.NoPK {
		if o.Desc() {
			d--
		} else {
			d++
		}
	}
	for _, b := range bands {
		if d >= b.LowerMs && d < b.UpperMs {
			return b.Class
		}
	}
	if d < 0 {
		return bands[len(bands)-1].Class
	}
	return bands[0].Class
}
