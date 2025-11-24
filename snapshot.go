package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v2/internal/index"
)

func (m *Client) saveSnapshot(ctx context.Context, catalog string, startTsSeq, stopTsSeq index.TimeSeqID, snapData []byte) (string, error) {
	return m.snapFlight.Do(fmt.Sprintf("%s_%s_%s", catalog, startTsSeq.String(), stopTsSeq.String()), func() (string, error) {
		return m._save(ctx, catalog, startTsSeq, stopTsSeq, snapData)
	})
}

func (m *Client) _save(ctx context.Context, catalog string, startTsSeq, stopTsSeq index.TimeSeqID, snapData []byte) (string, error) {
	snapKey := m.storage.MakeSnapKey(catalog, startTsSeq, stopTsSeq)
	if err := m.storage.Put(ctx, snapKey, snapData); err != nil {
		return "", fmt.Errorf("failed to save snapshot: %w", err)
	}

	// Add snapshot to index with time range
	if err := m.writer.AddSnap(ctx, catalog, startTsSeq, stopTsSeq); err != nil {
		return "", fmt.Errorf("failed to add snapshot to index: %w", err)
	}
	return snapKey, nil
}
