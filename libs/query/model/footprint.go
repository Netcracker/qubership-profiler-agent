package model

import "unsafe"

// RowOverheadBytes approximates the per-row (and per-map-entry) Go
// allocations the byte counts cannot see: string headers, the params map
// header and buckets, slice headers. Part of the §7.5 read-budget accounting
// model, shared by the cold and hot tiers so their ledgers agree.
const RowOverheadBytes = 192

// RowFootprint is the accounting size of one materialized CallRow
// (02-read-contract.md §7.5): the struct, the container overhead, and every
// string byte the row owns. Both tiers charge and reconcile with this one
// function, so bytes transferred between leases are always covered.
func RowFootprint(r *CallRow) int64 {
	n := int64(unsafe.Sizeof(CallRow{})) + RowOverheadBytes
	n += int64(len(r.PK.PodNamespace) + len(r.PK.PodService) + len(r.PK.PodName))
	n += int64(len(r.Method) + len(r.ThreadName) + len(r.RetentionClass) + len(r.TruncatedReason))
	for k, vs := range r.Params {
		n += int64(len(k)) + RowOverheadBytes
		for _, v := range vs {
			n += int64(len(v))
		}
	}
	return n
}
