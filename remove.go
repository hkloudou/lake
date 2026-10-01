package lake

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/utils"
	"github.com/redis/go-redis/v9"
)

// RemoveDelta is the operator's escape hatch for a poison delta — one whose
// body cannot be merged and therefore fails every read of the catalog. The
// merge error names its tsSeq ("{timestamp}_{seqid}") exactly for this call.
// Only the index entry goes; the body object stays. Returns whether an entry
// was removed.
//
// The removal bumps the catalog's removal generation atomically, so a read
// in flight that listed the removed delta can neither persist a snapshot nor
// have its cached sample served afterwards (both carry the generation they
// were computed under). Snapshots that already absorbed the delta keep it:
// this unblocks the log, it does not rewrite history.
//
// DESTRUCTIVE: the removed write disappears from every future read.
func (c *Client) RemoveDelta(ctx context.Context, catalog, tsSeq string) (bool, error) {
	c.emitEvent(catalog, "RemoveDelta", map[string]any{"tsSeq": tsSeq})
	if err := utils.ValidateCatalog(catalog); err != nil {
		return false, err
	}
	id, err := index.ParseTimeSeqID(tsSeq)
	if err != nil {
		return false, err
	}
	return c.idx.RemoveDelta(ctx, catalog, id)
}

// sweepSamples deletes the catalog's field from every memo hash
// ("<prefix>:m:*", found via SCAN so the server is never blocked). Stale
// entries are already rejected at read time by their generation; this just
// reclaims their memory. Errors are collected, not fatal to the sweep.
func (c *Client) sweepSamples(ctx context.Context, catalog string) error {
	pattern := globEscape(c.idx.Prefix()) + ":m:*"
	var (
		cursor uint64
		errs   []error
	)
	for {
		keys, next, err := c.sampleRdb.Scan(ctx, cursor, pattern, 256).Result()
		if err != nil {
			return errors.Join(append(errs, fmt.Errorf("scan %q: %w", pattern, err))...)
		}
		if len(keys) > 0 {
			pipe := c.sampleRdb.Pipeline()
			cmds := make([]*redis.IntCmd, len(keys))
			for i, key := range keys {
				cmds[i] = pipe.HDel(ctx, key, catalog)
			}
			_, _ = pipe.Exec(ctx)
			for i, cmd := range cmds {
				if err := cmd.Err(); err != nil {
					errs = append(errs, fmt.Errorf("hdel %s: %w", keys[i], err))
				}
			}
		}
		if next == 0 {
			return errors.Join(errs...)
		}
		cursor = next
	}
}

// globEscape makes s match only itself in a Redis MATCH pattern (the prefix
// is user-supplied and may contain glob metacharacters).
func globEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '*', '?', '[', ']', '\\':
			b.WriteByte('\\')
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
