package lake

import (
	"context"
	"fmt"
	"strings"

	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/trace"
)

// ListResult represents the read result
type ListResult struct {
	client     *Client           // Client instance
	catalog    string            // Catalog name
	LatestSnap *index.SnapInfo   // Snapshot info (if generated or used)
	Entries    []index.DeltaInfo // Raw entries (for debugging)
	HasPending bool              // True if pending writes detected (< 60s)
	Err        error             // Error if pending writes detected (non-fatal)
}

func (m ListResult) Exist() bool {
	// return m.LatestSnap != nil || len(m.Entries) > 0 || m.HasPending //removed pending writes for now
	return m.LatestSnap != nil || len(m.Entries) > 0
}

// Dump returns a debug string representation of the ListResult
func (m ListResult) Dump() string {
	var output strings.Builder

	// output.WriteString("=== List Result Debug Info ===\n")

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

	// output.WriteString("\n")

	// Entries info
	if len(m.Entries) > 0 {
		// output.WriteString(fmt.Sprintf("Entries: %d total\n", len(m.Entries)))
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

	// output.WriteString("\n=============================")

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
			// Score:      m.Entries[len(m.Entries)-1].TsSeq.Score(),
		}
	}

	return &index.SnapInfo{
		StartTsSeq: m.LatestSnap.StopTsSeq,
		StopTsSeq:  m.Entries[len(m.Entries)-1].TsSeq,
		// Score:      m.Entries[len(m.Entries)-1].TsSeq.Score(),
	}
}

// List reads catalog metadata and returns ListResult
// Errors (including pending writes) are stored in ListResult.Err
func (c *Client) List(ctx context.Context, catalog string) *ListResult {
	tr := trace.FromContext(ctx)

	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return &ListResult{
			client:  c,
			catalog: catalog,
			Err:     err,
		}
	}
	tr.RecordSpan("List.Init")

	// Try to get existing snapshot
	snap, err := c.reader.GetLatestSnap(ctx, catalog)
	if err != nil {
		return &ListResult{
			client:  c,
			catalog: catalog,
			Err:     fmt.Errorf("failed to get snapshot: %w", err),
		}
	}
	tr.RecordSpan("List.GetLatestSnap")

	var readResult *index.ReadIndexResult

	if snap != nil {
		readResult = c.reader.ReadSince(ctx, catalog, snap.StopTsSeq.Score())
	} else {
		// No snapshot, read all
		readResult = c.reader.ReadAll(ctx, catalog)
	}
	tr.RecordSpan("List.ReadIndex", map[string]interface{}{
		"count":      len(readResult.Deltas),
		"hasPending": readResult.HasPending,
	})

	return &ListResult{
		client:     c,
		catalog:    catalog,
		LatestSnap: snap,
		Entries:    readResult.Deltas,
		HasPending: readResult.HasPending,
		Err:        readResult.Err,
	}
}
