package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/utils"
)

// RemoveDelta is the operator's escape hatch for a poison delta — one whose
// body cannot be merged (invalid JSON, unappliable patch) and therefore
// fails EVERY read of the catalog. The merge error names the offending
// delta's tsSeq exactly for this call:
//
//	merge failed (path=/profile tsSeq=1700000000_42 uri=... type=1): ...
//	→ removed, err := client.RemoveDelta(ctx, "users", "1700000000_42")
//
// tsSeq is that "{timestamp}_{seqid}" string, verbatim. Only Redis state is
// touched — the body object in storage remains. Returns whether an entry was
// actually removed (false: no delta at that tsSeq, e.g. already removed or
// compacted).
//
// Removal is coherent with derived state:
//
//   - snapshots: the removal bumps the catalog's removal generation in the
//     same atomic step, and AddSnap refuses a snapshot computed from an
//     earlier generation — so a read that was in flight (and had listed the
//     removed delta) cannot persist its effect. Snapshots that ALREADY
//     absorbed the delta before this call keep it — RemoveDelta unblocks the
//     log, it does not rewrite history.
//   - samples: every indicator's memo entry for the catalog is invalidated
//     (epoch-bumped, so in-flight sample computes cannot write stale values
//     back either). On sweep failure the delta is still removed; the error
//     tells the operator to retry InvalidateSamples per indicator.
//
// DESTRUCTIVE: the removed delta's write disappears from every future read.
// That is the point — the delta was blocking the catalog — but it is not an
// undo mechanism for healthy writes.
func (c *Client) RemoveDelta(ctx context.Context, catalog, tsSeq string) (bool, error) {
	c.emitEvent(catalog, "RemoveDelta", map[string]any{"tsSeq": tsSeq})
	if err := utils.ValidateCatalog(catalog); err != nil {
		return false, err
	}
	id, err := index.ParseTimeSeqID(tsSeq)
	if err != nil {
		return false, err
	}
	removed, err := c.writer.RemoveDelta(ctx, catalog, id)
	if err != nil || !removed {
		return removed, err
	}
	if err := c.invalidateAllSamples(ctx, catalog); err != nil {
		return true, fmt.Errorf("delta removed, but sample invalidation failed (retry InvalidateSamples per indicator): %w", err)
	}
	return true, nil
}

// invalidateAllSamples voids the catalog's sample state across ALL
// indicators after a removal, in two steps whose order matters:
//
//  1. Bump the catalog's removal generation ("<prefix>:mrg"). Every memo
//     write is conditional on the generation it observed at probe time, so
//     this instantly voids in-flight computes for every indicator —
//     including one whose memo hash does not exist yet (a first-ever Sample
//     racing the removal), which no key scan could reach.
//  2. Delete the catalog's entry from every EXISTING memo hash (SCAN
//     "<prefix>:m:*"; never blocks the server on the full keyspace).
//     Necessary because a removal can lower LastUpdated(), so the staleness
//     floor would keep serving an already-cached entry forever.
func (c *Client) invalidateAllSamples(ctx context.Context, catalog string) error {
	if err := c.sampleRdb.HIncrBy(ctx, c.reader.MakeSampleRemoveGenKey(), catalog, 1).Err(); err != nil {
		return fmt.Errorf("bump sample removal gen: %w", err)
	}
	pattern := c.reader.Prefix() + ":m:*"
	var cursor uint64
	for {
		keys, next, err := c.sampleRdb.Scan(ctx, cursor, pattern, 256).Result()
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := c.sampleRdb.HDel(ctx, key, catalog).Err(); err != nil {
				return fmt.Errorf("invalidate %s: %w", key, err)
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}
