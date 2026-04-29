package lake

import (
	"context"
	"fmt"
	"strings"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/utils"
)

// ListResult is the read-side view of a catalog: the latest snap (if any)
// and the deltas after it.
type ListResult struct {
	client     *Client
	catalog    string
	LatestSnap *index.SnapInfo
	Entries    []index.DeltaInfo
	HasPending bool  // a pending write younger than 120s overlaps the result
	Err        error // Redis / decode failures
}

// LastUpdated is the score of the most recent observable change.
func (m ListResult) LastUpdated() float64 {
	if len(m.Entries) > 0 {
		return m.Entries[len(m.Entries)-1].Score
	}
	if m.LatestSnap != nil {
		return m.LatestSnap.StopTsSeq.Score()
	}
	return 0
}

// Exist reports whether the catalog has any persisted state.
func (m ListResult) Exist() bool {
	return m.LatestSnap != nil || len(m.Entries) > 0
}

// HasNextSnap reports whether NextSnap would return a non-nil value.
func (m ListResult) HasNextSnap() bool { return len(m.Entries) > 0 }

// NextSnap is the snap that should next be persisted for this list, or
// nil if there are no entries past the latest snap.
func (m ListResult) NextSnap() *index.SnapInfo {
	if len(m.Entries) == 0 {
		return nil
	}
	return &index.SnapInfo{StopTsSeq: m.Entries[len(m.Entries)-1].TsSeq}
}

// Dump renders a debug-friendly description of the result.
func (m ListResult) Dump() string {
	var b strings.Builder
	if m.LatestSnap != nil {
		b.WriteString("Latest Snapshot:\n")
		b.WriteString(m.LatestSnap.Dump())
		if m.HasNextSnap() {
			b.WriteString("Next Snapshot:\n")
			b.WriteString(m.NextSnap().Dump())
		}
	} else {
		b.WriteString("No snapshot found\n")
	}
	if len(m.Entries) == 0 {
		b.WriteString("No entries found\n")
		return b.String()
	}
	for i, e := range m.Entries {
		fmt.Fprintf(&b, "\n[%d/%d] --------------------------------\n", i+1, len(m.Entries))
		fmt.Fprintf(&b, "  Path: %s\n", e.Path)
		fmt.Fprintf(&b, "  TsSeq: %s\n", e.TsSeq)
		fmt.Fprintf(&b, "  MergeType: %d (%s)\n", e.MergeType, e.MergeType.String())
		fmt.Fprintf(&b, "  Score: %.6f\n", e.Score)
	}
	return b.String()
}

type listOption struct {
	strictPending bool
}

// ListOption configures List behavior.
type ListOption func(*listOption)

// WithStrictPending makes List flag HasPending for any pending member,
// not just those followed by a delta.
func WithStrictPending() ListOption { return func(o *listOption) { o.strictPending = true } }

// List reads the catalog metadata in two Redis ops (HGet snap + ZRange
// deltas). Errors land in ListResult.Err; the "List" event fires before
// any early return.
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
		return &ListResult{client: c, catalog: catalog, Err: err}
	}

	snap, err := c.reader.GetLatestSnap(ctx, catalog)
	if err != nil {
		return &ListResult{client: c, catalog: catalog, Err: fmt.Errorf("get snapshot: %w", err)}
	}
	var rr *index.ReadIndexResult
	if snap != nil {
		rr = c.reader.ReadSince(ctx, catalog, snap.StopTsSeq.Score(), opt.strictPending)
	} else {
		rr = c.reader.ReadAll(ctx, catalog, opt.strictPending)
	}
	return &ListResult{
		client:     c,
		catalog:    catalog,
		LatestSnap: snap,
		Entries:    rr.Deltas,
		HasPending: rr.HasPending,
		Err:        rr.Err,
	}
}

// BatchList runs List for many catalogs in 2 Redis round-trips total
// (one HMGet on snaps + one pipelined ZRange). Each catalog's result is
// independent; an invalid catalog name only poisons its own ListResult.
func (c *Client) BatchList(ctx context.Context, catalogs []string, opts ...ListOption) map[string]*ListResult {
	var opt listOption
	for _, o := range opts {
		o(&opt)
	}

	out := make(map[string]*ListResult, len(catalogs))
	for _, cat := range catalogs {
		c.emitEvent(cat, "BatchList", nil)
	}

	valid := make([]string, 0, len(catalogs))
	for _, cat := range catalogs {
		if err := utils.ValidateCatalog(cat); err != nil {
			out[cat] = &ListResult{client: c, catalog: cat, Err: err}
			continue
		}
		valid = append(valid, cat)
	}

	if err := c.ensureInitialized(ctx); err != nil {
		for _, cat := range valid {
			out[cat] = &ListResult{client: c, catalog: cat, Err: err}
		}
		return out
	}

	results := c.reader.BatchList(ctx, valid, opt.strictPending)
	for _, cat := range valid {
		br := results[cat]
		lr := &ListResult{client: c, catalog: cat}
		if br.ReadResult != nil && br.ReadResult.Err != nil {
			lr.Err = br.ReadResult.Err
		} else {
			lr.LatestSnap = br.Snap
			if br.ReadResult != nil {
				lr.Entries = br.ReadResult.Deltas
				lr.HasPending = br.ReadResult.HasPending
			}
		}
		out[cat] = lr
	}
	return out
}
