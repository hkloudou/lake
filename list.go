package lake

import (
	"context"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/utils"
)

// ListResult is the read-side view of a catalog: the latest snap (if any)
// and the deltas after it, observed atomically.
type ListResult struct {
	client     *Client
	catalog    string
	removeGen  string // removal generation at list time; guards snapshot saves and sample writes
	LatestSnap *index.SnapInfo
	Entries    []index.DeltaInfo
	Err        error
}

// LastUpdated is the score of the most recent observable change.
func (m ListResult) LastUpdated() float64 {
	if n := len(m.Entries); n > 0 {
		return m.Entries[n-1].Score
	}
	if m.LatestSnap != nil {
		return m.LatestSnap.Score()
	}
	return 0
}

// Exist reports whether the catalog has any persisted state.
func (m ListResult) Exist() bool { return m.LatestSnap != nil || len(m.Entries) > 0 }

// RemoveGen is the catalog's removal generation observed with this list
// ("0" until the first RemoveDelta). A removal can lower or preserve
// LastUpdated, so cross-catalog samplers compare this too.
func (m ListResult) RemoveGen() string {
	if m.removeGen == "" {
		return "0"
	}
	return m.removeGen
}

// List reads the catalog's snap pointer and the deltas past it in one atomic
// Redis op (so an operator trimming absorbed entries can never race a read).
func (c *Client) List(ctx context.Context, catalog string) *ListResult {
	c.emitEvent(catalog, "List", nil)
	if err := utils.ValidateCatalog(catalog); err != nil {
		return &ListResult{client: c, catalog: catalog, Err: err}
	}
	return c.toListResult(catalog, c.idx.List(ctx, catalog))
}

// BatchList runs List for many catalogs in one pipelined round-trip.
func (c *Client) BatchList(ctx context.Context, catalogs []string) map[string]*ListResult {
	out := make(map[string]*ListResult, len(catalogs))
	valid := make([]string, 0, len(catalogs))
	for _, cat := range catalogs {
		c.emitEvent(cat, "BatchList", nil)
		if err := utils.ValidateCatalog(cat); err != nil {
			out[cat] = &ListResult{client: c, catalog: cat, Err: err}
			continue
		}
		valid = append(valid, cat)
	}
	for cat, l := range c.idx.BatchList(ctx, valid) {
		out[cat] = c.toListResult(cat, l)
	}
	return out
}

func (c *Client) toListResult(catalog string, l index.Listing) *ListResult {
	return &ListResult{client: c, catalog: catalog, removeGen: l.RemoveGen, LatestSnap: l.Snap, Entries: l.Deltas, Err: l.Err}
}
