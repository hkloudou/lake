package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/index"
)

// AllSnaps returns the latest snap metadata for every catalog in this
// deployment in a single Redis HGETALL on "<prefix>:snaps". This is the
// canonical entry point for whole-deployment backup tooling: feed the
// (catalog, StartTsSeq, StopTsSeq) triples into the storage backend's
// MakeSnapKey to produce the OSS object key for every snap — no OSS
// LIST needed.
//
// Sample usage:
//
//	all, err := client.AllSnaps(ctx)
//	for catalog, info := range all {
//	    ossKey := storage.MakeSnapKey(catalog, info.StartTsSeq, info.StopTsSeq)
//	    // copy ossKey to the backup bucket ...
//	}
func (c *Client) AllSnaps(ctx context.Context) (map[string]SnapInfo, error) {
	if err := c.ensureInitialized(ctx); err != nil {
		return nil, err
	}
	out, err := c.reader.AllSnaps(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read snaps hash: %w", err)
	}
	return out, nil
}

// saveSnapshot writes the snapshot bytes to storage and upserts the snap
// metadata in Redis. The previous snap entry for the same catalog is
// overwritten in Redis; its OSS object is left orphan (acceptable per
// the V3 contract — see clear_optimized.go).
//
// SingleFlight on (catalog, stop) deduplicates concurrent saves of the
// same snap point computed by independent readers.
func (m *Client) saveSnapshot(ctx context.Context, catalog string, stopTsSeq index.TimeSeqID, snapData []byte) (string, error) {
	return m.snapFlight.Do(fmt.Sprintf("%s_%s", catalog, stopTsSeq.String()), func() (string, error) {
		return m._save(ctx, catalog, stopTsSeq, snapData)
	})
}

func (m *Client) _save(ctx context.Context, catalog string, stopTsSeq index.TimeSeqID, snapData []byte) (string, error) {
	snapKey := m.storage.MakeSnapKey(catalog, stopTsSeq)
	if err := m.storage.Put(ctx, snapKey, snapData); err != nil {
		return "", fmt.Errorf("failed to save snapshot: %w", err)
	}
	if err := m.writer.AddSnap(ctx, catalog, stopTsSeq); err != nil {
		return "", fmt.Errorf("failed to add snapshot to index: %w", err)
	}
	return snapKey, nil
}
