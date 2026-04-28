package lake

import (
	"context"
	"fmt"
	"strings"

	"github.com/hkloudou/lake/v3/internal/index"
)

// ListResult represents the read result
type ListResult struct {
	client     *Client           // Client instance
	catalog    string            // Catalog name
	LatestSnap *index.SnapInfo   // Snapshot info (if generated or used)
	Entries    []index.DeltaInfo // Raw entries (for debugging)
	HasPending bool              // True if pending writes detected (< 60s)
	Err        error             // Error from index read (Redis/decode failures)
}

func (m ListResult) LastUpdated() float64 {
	if len(m.Entries) > 0 {
		return m.Entries[len(m.Entries)-1].Score
	}
	if m.LatestSnap != nil {
		return m.LatestSnap.StopTsSeq.Score()
	}
	return 0
}

func (m ListResult) Exist() bool {
	// return m.LatestSnap != nil || len(m.Entries) > 0 || m.HasPending //removed pending writes for now
	return m.LatestSnap != nil || len(m.Entries) > 0
}

// Dump returns a debug string representation of the ListResult
func (m ListResult) Dump() string {
	var output strings.Builder

	// Snapshot info
	if m.LatestSnap != nil {
		output.WriteString("Latest Snapshot:\n")
		output.WriteString(m.LatestSnap.Dump())
		if m.HasNextSnap() {
			output.WriteString("Next Snapshot:\n")
			output.WriteString(m.NextSnap().Dump())
		}
	} else {
		output.WriteString("No snapshot found\n")
	}

	// Entries info
	if len(m.Entries) > 0 {
		for i, entry := range m.Entries {
			output.WriteString(fmt.Sprintf("\n[%d/%d] --------------------------------\n", i+1, len(m.Entries)))
			output.WriteString(fmt.Sprintf("  Path: %s\n", entry.Path))
			output.WriteString(fmt.Sprintf("  TsSeq: %s\n", entry.TsSeq))
			output.WriteString(fmt.Sprintf("  MergeType: %d (%s)\n", entry.MergeType, entry.MergeType.String()))
			output.WriteString(fmt.Sprintf("  Score: %.6f\n", entry.Score))
		}
	} else {
		output.WriteString("No entries found\n")
	}

	return output.String()
}

func (m ListResult) HasNextSnap() bool {
	return len(m.Entries) > 0
}

func (m ListResult) NextSnap() *index.SnapInfo {
	if len(m.Entries) == 0 {
		return nil
	}
	if m.LatestSnap == nil {
		return &index.SnapInfo{
			StartTsSeq: index.TimeSeqID{Timestamp: 0, SeqID: 0},
			StopTsSeq:  m.Entries[len(m.Entries)-1].TsSeq,
		}
	}

	return &index.SnapInfo{
		StartTsSeq: m.LatestSnap.StopTsSeq,
		StopTsSeq:  m.Entries[len(m.Entries)-1].TsSeq,
	}
}

type listOption struct {
	strictPending bool
}

// ListOption configures List behavior
type ListOption func(*listOption)

// WithStrictPending makes List report HasPending=true for any pending member,
// regardless of position. By default, only pending members followed by a delta trigger HasPending.
func WithStrictPending() ListOption {
	return func(o *listOption) {
		o.strictPending = true
	}
}

// BatchList performs List operations for multiple catalogs in 2 Redis round-trips.
// Each catalog's result is independent — one catalog's error does not affect others.
func (c *Client) BatchList(ctx context.Context, catalogs []string, opts ...ListOption) map[string]*ListResult {
	var opt listOption
	for _, o := range opts {
		o(&opt)
	}

	result := make(map[string]*ListResult, len(catalogs))

	for _, catalog := range catalogs {
		c.emitEvent(catalog, "BatchList", nil)
	}

	if err := c.ensureInitialized(ctx); err != nil {
		for _, catalog := range catalogs {
			result[catalog] = &ListResult{client: c, catalog: catalog, Err: err}
		}
		return result
	}

	batchResults := c.reader.BatchList(ctx, catalogs, opt.strictPending)

	for _, catalog := range catalogs {
		br := batchResults[catalog]
		lr := &ListResult{
			client:  c,
			catalog: catalog,
		}
		if br.ReadResult != nil && br.ReadResult.Err != nil {
			lr.Err = br.ReadResult.Err
		} else {
			lr.LatestSnap = br.Snap
			if br.ReadResult != nil {
				lr.Entries = br.ReadResult.Deltas
				lr.HasPending = br.ReadResult.HasPending
			}
		}
		result[catalog] = lr
	}

	return result
}

// List reads catalog metadata and returns ListResult
// Errors (including pending writes) are stored in ListResult.Err
func (c *Client) List(ctx context.Context, catalog string, opts ...ListOption) *ListResult {
	var opt listOption
	for _, o := range opts {
		o(&opt)
	}

	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return &ListResult{
			client:  c,
			catalog: catalog,
			Err:     err,
		}
	}

	c.emitEvent(catalog, "List", nil)

	// Try to get existing snapshot
	snap, err := c.reader.GetLatestSnap(ctx, catalog)
	if err != nil {
		return &ListResult{
			client:  c,
			catalog: catalog,
			Err:     fmt.Errorf("failed to get snapshot: %w", err),
		}
	}

	var readResult *index.ReadIndexResult

	if snap != nil {
		readResult = c.reader.ReadSince(ctx, catalog, snap.StopTsSeq.Score(), opt.strictPending)
	} else {
		readResult = c.reader.ReadAll(ctx, catalog, opt.strictPending)
	}

	return &ListResult{
		client:     c,
		catalog:    catalog,
		LatestSnap: snap,
		Entries:    readResult.Deltas,
		HasPending: readResult.HasPending,
		Err:        readResult.Err,
	}
}
