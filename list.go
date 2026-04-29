package lake

import (
	"context"
	"fmt"
	"strings"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/utils"
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

// NextSnap returns the SnapInfo describing the snap that should next be
// generated for this list, or nil if there is nothing new since the
// latest snap (no entries past the snap point).
//
// V3 only stores stopTsSeq, so the constructed SnapInfo carries only the
// last entry's tsSeq.
func (m ListResult) NextSnap() *index.SnapInfo {
	if len(m.Entries) == 0 {
		return nil
	}
	return &index.SnapInfo{
		StopTsSeq: m.Entries[len(m.Entries)-1].TsSeq,
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

	// Per-catalog validation. Bad names get an Err in their ListResult but
	// do not poison the rest of the batch. Valid names move on to the
	// pipeline call below.
	valid := make([]string, 0, len(catalogs))
	for _, catalog := range catalogs {
		if err := utils.ValidateCatalog(catalog); err != nil {
			result[catalog] = &ListResult{client: c, catalog: catalog, Err: err}
			continue
		}
		valid = append(valid, catalog)
	}

	if err := c.ensureInitialized(ctx); err != nil {
		for _, catalog := range valid {
			result[catalog] = &ListResult{client: c, catalog: catalog, Err: err}
		}
		return result
	}

	batchResults := c.reader.BatchList(ctx, valid, opt.strictPending)

	for _, catalog := range valid {
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

// List reads catalog metadata and returns ListResult.
// Errors (including pending writes) are stored in ListResult.Err.
//
// Event contract: the "List" event is emitted unconditionally before any
// early return so EventHandlers can observe every call attempt, including
// those that fail at initialization.
func (c *Client) List(ctx context.Context, catalog string, opts ...ListOption) *ListResult {
	c.emitEvent(catalog, "List", nil)

	var opt listOption
	for _, o := range opts {
		o(&opt)
	}

	if err := utils.ValidateCatalog(catalog); err != nil {
		return &ListResult{client: c, catalog: catalog, Err: err}
	}

	if err := c.ensureInitialized(ctx); err != nil {
		return &ListResult{
			client:  c,
			catalog: catalog,
			Err:     err,
		}
	}

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
