package lake

import (
	"fmt"
	"strings"

	"github.com/hkloudou/lake/v2/internal/index"
)

// ListResult represents the read result
type ListResult struct {
	client     *Client           // Client instance
	catalog    string            // Catalog name
	LatestSnap *index.SnapInfo   // Snapshot info (if generated or used)
	Entries    []index.DeltaInfo // Raw entries (for debugging)
	Err        error             // Error if pending writes detected (non-fatal)
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
			output.WriteString(fmt.Sprintf("  Field: %s\n", entry.Field))
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

func (m ListResult) NextSnap() index.SnapInfo {
	if m.LatestSnap == nil {
		return index.SnapInfo{
			StartTsSeq: index.TimeSeqID{Timestamp: 0, SeqID: 0},
			StopTsSeq:  m.Entries[len(m.Entries)-1].TsSeq,
			// Score:      m.Entries[len(m.Entries)-1].TsSeq.Score(),
		}
	}

	return index.SnapInfo{
		StartTsSeq: m.LatestSnap.StopTsSeq,
		StopTsSeq:  m.Entries[len(m.Entries)-1].TsSeq,
		// Score:      m.Entries[len(m.Entries)-1].TsSeq.Score(),
	}
}
