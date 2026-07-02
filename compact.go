package lake

import (
	"context"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// Compact removes the index entries of every delta the catalog's current
// snapshot has already absorbed (delta score ≤ snap stop, inclusive — the
// read path only ever fetches deltas strictly after the stop). It returns
// the number of entries removed.
//
// Scope: Redis only. The delta OBJECTS in storage are untouched — they
// remain fetchable history, and object deletion belongs to bucket lifecycle
// rules, not Lake. Compacting changes no read result, only reclaims index
// memory.
//
// Safe to call at any time, from any process:
//
//   - reads observe (snap pointer, deltas after it) atomically in one script,
//     so a concurrent read either ran before this compaction (and already has
//     its deltas) or after (and starts from a snap that covers them) — there
//     is no interleaving that loses the range in between;
//   - the snap pointer is monotonic (AddSnap), so the bound this call trims
//     to can never move backwards past a delta some reader still needs.
//
// A catalog with no snapshot (or an undecodable snap entry) is left intact
// and returns (0, nil). Compaction is explicit by design — there is no
// background reaper; sweep catalogs on your own schedule, e.g. via
// IterateSnaps.
func (c *Client) Compact(ctx context.Context, catalog string) (int64, error) {
	c.emitEvent(catalog, "Compact", nil)
	if err := utils.ValidateCatalog(catalog); err != nil {
		return 0, err
	}
	return c.writer.CompactDeltas(ctx, catalog)
}
