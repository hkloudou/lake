package lake

import (
	"context"
	"fmt"
	"strings"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/utils"
)

// WriteRequest carries one write operation.
type WriteRequest struct {
	Catalog   string
	Path      string    // JSON path; "/" means root
	Body      []byte    // raw JSON
	MergeType MergeType // Replace, RFC7396, or RFC6902
}

// Write applies a JSON delta via three-phase commit:
//
//  1. Lua: ZADD pending|... + INCR seqid (atomic in Redis).
//  2. storage.Put(deltaKey, body).
//  3. Lua: ZREM pending|... + ZADD delta|... (atomic in Redis).
//
// Failures between (2) and (3) leave a storage object without a
// committed Redis entry — an accepted orphan. Pending members age out
// after 120s and are filtered by the reader; ClearHistory reclaims the
// orphan storage object on next sweep.
func (c *Client) Write(ctx context.Context, req WriteRequest) error {
	c.emitEvent(req.Catalog, "Write", map[string]any{"path": req.Path, "mergeType": int(req.MergeType)})

	if err := utils.ValidateCatalog(req.Catalog); err != nil {
		return err
	}
	if err := utils.ValidateFieldPath(req.Path); err != nil {
		return err
	}
	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}

	tsSeq, pendingMember, err := c.writer.GetTimeSeqIDAndPreCommit(ctx, req.Catalog, req.Path, req.MergeType)
	if err != nil {
		return fmt.Errorf("precommit: %w", err)
	}

	deltaKey := c.storage.MakeDeltaKey(req.Catalog, tsSeq, int(req.MergeType))
	if err := c.storage.Put(ctx, deltaKey, req.Body); err != nil {
		c.writer.Rollback(ctx, req.Catalog, pendingMember)
		return fmt.Errorf("storage put: %w", err)
	}

	committedMember := index.EncodeDeltaMember(req.Path, req.MergeType, tsSeq)
	if strings.TrimPrefix(pendingMember, "pending|") != committedMember {
		return fmt.Errorf("pending/committed member mismatch: %q != %q", pendingMember, committedMember)
	}
	if err := c.writer.Commit(ctx, req.Catalog, pendingMember, committedMember, tsSeq.Score()); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
