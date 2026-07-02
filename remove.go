package lake

import (
	"context"
	"fmt"
	"strings"

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
//   - samples: the catalog's sample removal generation is bumped BEFORE the
//     removal (in-flight computes for any indicator — even one that has
//     never cached — cannot write pre-removal state back), cached entries
//     carry the generation they were computed under and are rejected on
//     generation mismatch at read time, and every indicator's memo entry is
//     swept eagerly. A sweep failure therefore costs memory, not
//     correctness.
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
	// Install the sample write barrier BEFORE removing anything: if the bump
	// failed after the ZREM (e.g. a separate sample-cache Redis briefly
	// down), the delta would be gone with the barrier at the old generation,
	// an in-flight first-ever sampler could still cache pre-removal state,
	// and a retry would return false without ever installing the barrier. A
	// bump whose removal then fails is harmless — it only discards some
	// in-flight cache writes.
	if err := c.sampleRdb.HIncrBy(ctx, c.reader.MakeSampleRemoveGenKey(), catalog, 1).Err(); err != nil {
		return false, fmt.Errorf("install sample barrier: %w", err)
	}
	removed, err := c.writer.RemoveDelta(ctx, catalog, id)
	if err != nil || !removed {
		return removed, err
	}
	if err := c.sweepSamples(ctx, catalog); err != nil {
		// Correctness no longer depends on the sweep (stale entries carry an
		// older generation and are rejected at read time); failing here only
		// leaves memory to reclaim.
		return true, fmt.Errorf("delta removed, but memo sweep failed (entries expire from reads; retry InvalidateSamples to reclaim now): %w", err)
	}
	return true, nil
}

// sweepSamples deletes the catalog's entry from every EXISTING memo hash
// (SCAN "<prefix>:m:*"; never blocks the server on the full keyspace). The
// write barrier ("<prefix>:mrg", bumped by RemoveDelta before the removal)
// already voids in-flight computes for every indicator — including ones
// whose memo hash does not exist yet, which no key scan could reach — and
// the per-entry generation check rejects unswept entries at read time; this
// sweep just reclaims their memory eagerly.
func (c *Client) sweepSamples(ctx context.Context, catalog string) error {
	// The prefix is user-supplied and MATCH treats *?[]\ as glob syntax —
	// unescaped, a prefix like "app[1]" would silently match the wrong keys
	// (missing this deployment's memo hashes, or sweeping another's).
	pattern := globEscape(c.reader.Prefix()) + ":m:*"
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

// globEscape backslash-escapes Redis MATCH metacharacters so s matches only
// itself as a literal pattern segment.
func globEscape(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '*', '?', '[', ']', '\\':
			b.WriteByte('\\')
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
