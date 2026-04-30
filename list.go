package lake

import (
	"context"
	"fmt"
	"strings"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/utils"
)

// ListResult is the read-side view of a catalog: the latest snap (if
// any) and the deltas after it.
type ListResult struct {
	client     *Client
	catalog    string
	LatestSnap *index.SnapInfo
	Entries    []index.DeltaInfo
	Err        error
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

func (m ListResult) HasNextSnap() bool { return len(m.Entries) > 0 }

// NextSnap is the snap that should next be persisted, or nil if there
// are no entries past the latest snap.
func (m ListResult) NextSnap() *index.SnapInfo {
	if len(m.Entries) == 0 {
		return nil
	}
	return &index.SnapInfo{StopTsSeq: m.Entries[len(m.Entries)-1].TsSeq}
}

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
		fmt.Fprintf(&b, "  UUID: %s\n", e.UUID)
		fmt.Fprintf(&b, "  MergeType: %d (%s)\n", e.MergeType, e.MergeType.String())
		fmt.Fprintf(&b, "  Score: %.6f\n", e.Score)
	}
	return b.String()
}

// List reads the catalog metadata in two Redis ops (HGet snap + ZRange
// deltas).
func (c *Client) List(ctx context.Context, catalog string) *ListResult {
	c.emitEvent(catalog, "List", nil)

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
		rr = c.reader.ReadSince(ctx, catalog, snap.StopTsSeq.Score())
	} else {
		rr = c.reader.ReadAll(ctx, catalog)
	}
	return &ListResult{
		client:     c,
		catalog:    catalog,
		LatestSnap: snap,
		Entries:    rr.Deltas,
		Err:        rr.Err,
	}
}

// BatchList runs List for many catalogs in 2 Redis round-trips total.
func (c *Client) BatchList(ctx context.Context, catalogs []string) map[string]*ListResult {
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

	results := c.reader.BatchList(ctx, valid)
	for _, cat := range valid {
		br := results[cat]
		lr := &ListResult{client: c, catalog: cat}
		if br.ReadResult != nil && br.ReadResult.Err != nil {
			lr.Err = br.ReadResult.Err
		} else {
			lr.LatestSnap = br.Snap
			if br.ReadResult != nil {
				lr.Entries = br.ReadResult.Deltas
			}
		}
		out[cat] = lr
	}
	return out
}
