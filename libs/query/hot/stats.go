package hot

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	"github.com/pkg/errors"
)

// maxHotStatsBytes caps one /internal/v1/stats response. It carries one
// 8-byte hash per hot call in base64 plus at most PROFILER_STATS_MAX_GROUPS
// groups, so the cap allows tens of millions of calls per replica.
const maxHotStatsBytes = 512 << 20

// StatsResult is one replica's /internal/v1/stats answer, decoded.
type StatsResult struct {
	Groups      []stats.Group
	Totals      stats.Totals
	Approximate bool
	// Hashes are the PK hashes of the calls the replica counted.
	Hashes []uint64
}

// Stats fetches one replica's partial aggregates (02 §3, §2.9). The body is
// read through the read memory budget like a /calls page, and the returned
// lease ends up holding the decoded groups and 8 bytes per hash, the charge
// §2.9 names; the caller moves it into the request lease. On any error the
// lease is already released.
func (c *Client) Stats(ctx context.Context, baseURL string, params url.Values) (StatsResult, *budget.Lease, error) {
	u := baseURL + "/internal/v1/stats?" + params.Encode()
	raw, lease, err := c.getBudgeted(ctx, u, maxHotStatsBytes)
	if err != nil {
		return StatsResult{}, nil, err
	}
	// The raw body, the decoded body, and the hashes coexist until the
	// reconcile below; reserve for them before decoding, as Calls does.
	if err := lease.Grow(ctx, int64(len(raw))*hotDecodeReserveFactor); err != nil {
		lease.Release()
		return StatsResult{}, nil, err
	}
	var body stats.InternalResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		lease.Release()
		return StatsResult{}, nil, errors.Wrapf(err, "decode %s", u)
	}
	out := StatsResult{Totals: body.Totals, Approximate: body.Approximate, Groups: make([]stats.Group, 0, len(body.Groups))}
	if out.Hashes, err = stats.DecodeHashes(body.PKHashes); err != nil {
		lease.Release()
		return StatsResult{}, nil, errors.Wrapf(err, "decode %s", u)
	}
	footprint := 8 * int64(len(out.Hashes))
	for _, wg := range body.Groups {
		g, err := wg.Group()
		if err != nil {
			lease.Release()
			return StatsResult{}, nil, errors.Wrapf(err, "decode %s", u)
		}
		footprint += stats.GroupBytes + int64(len(g.Key)) + int64(g.Sketch.Bins())*stats.BinBytes
		out.Groups = append(out.Groups, g)
	}
	if held := lease.Held(); footprint > held {
		if err := lease.Grow(ctx, footprint-held); err != nil {
			lease.Release()
			return StatsResult{}, nil, err
		}
	} else {
		lease.Shrink(held - footprint)
	}
	return out, lease, nil
}
