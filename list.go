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
	removeGen  string // removal generation at list time; guards snapshot saves
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

// RemoveGen is the catalog's removal generation observed atomically with
// this list ("0" until the first RemoveDelta). Cross-catalog Samplers use it
// the same way they use LastUpdated: record the peer generations the sample
// depended on inside T, and have a WithShouldRefresh predicate compare them
// against peers[...].RemoveGen() — a removal on a peer can lower or preserve
// that peer's LastUpdated, so the version alone cannot reveal it.
func (m ListResult) RemoveGen() string {
	if m.removeGen == "" {
		return "0"
	}
	return m.removeGen
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
		fmt.Fprintf(&b, "  URI: %s\n", e.URI)
		fmt.Fprintf(&b, "  MergeType: %d (%s)\n", e.MergeType, e.MergeType.String())
		fmt.Fprintf(&b, "  Score: %.6f\n", e.Score)
	}
	return b.String()
}

// backlogWarnThreshold is where List starts flagging an unsnapshotted delta
// tail via the "ListLargeBacklog" event. The list script has no result cap —
// every read materialises and ships the whole tail — so a catalog that only
// ever accumulates (no snap target, or List/Sampler-only usage that never
// triggers the read-path snapshot) degrades silently until Redis stalls.
// The event gives operators the signal to configure a snap target or run
// Compact before that point.
const backlogWarnThreshold = 10_000

// List reads the catalog metadata in one atomic Redis op (a Lua script
// fetches the snap pointer and the deltas past it together — atomicity is
// what makes Compact safe to run concurrently with reads).
func (c *Client) List(ctx context.Context, catalog string) *ListResult {
	c.emitEvent(catalog, "List", nil)

	if err := utils.ValidateCatalog(catalog); err != nil {
		return &ListResult{client: c, catalog: catalog, Err: err}
	}

	snap, rr := c.reader.ListCatalog(ctx, catalog)
	if len(rr.Deltas) >= backlogWarnThreshold && c.hasHandlers() {
		c.emitEvent(catalog, "ListLargeBacklog", map[string]any{"entries": len(rr.Deltas)})
	}
	return &ListResult{
		client:     c,
		catalog:    catalog,
		removeGen:  rr.RemoveGen,
		LatestSnap: snap,
		Entries:    rr.Deltas,
		Err:        rr.Err,
	}
}

// BatchList runs List for many catalogs in one pipelined Redis round-trip.
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
				lr.removeGen = br.ReadResult.RemoveGen
				if len(lr.Entries) >= backlogWarnThreshold && c.hasHandlers() {
					c.emitEvent(cat, "ListLargeBacklog", map[string]any{"entries": len(lr.Entries)})
				}
			}
		}
		out[cat] = lr
	}
	return out
}
