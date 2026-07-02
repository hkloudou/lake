package lake

import (
	"context"

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
// tsSeq is that "{timestamp}_{seqid}" string, verbatim. Only the Redis index
// entry is removed — the body object in storage is untouched. Returns whether
// an entry was actually removed (false: no delta at that tsSeq, e.g. already
// removed or compacted).
//
// DESTRUCTIVE: the removed delta's write is dropped from every future read
// (past snapshots that already absorbed it are unaffected). This is the
// point — the delta was blocking the catalog — but it is not an undo for
// healthy writes.
func (c *Client) RemoveDelta(ctx context.Context, catalog, tsSeq string) (bool, error) {
	c.emitEvent(catalog, "RemoveDelta", map[string]any{"tsSeq": tsSeq})
	if err := utils.ValidateCatalog(catalog); err != nil {
		return false, err
	}
	id, err := index.ParseTimeSeqID(tsSeq)
	if err != nil {
		return false, err
	}
	return c.writer.RemoveDelta(ctx, catalog, id)
}
