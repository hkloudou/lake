package snapshot

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/internal/storage"
	"github.com/hkloudou/lake/v2/internal/xsync"
)

// Manager handles snapshot saving and reading
type Manager struct {
	storage storage.Storage
	reader  *index.Reader
	writer  *index.Writer
	flight  xsync.SingleFlight[string]
}

// NewManager creates a new snapshot manager
func NewManager(
	storage storage.Storage,
	reader *index.Reader,
	writer *index.Writer,
) *Manager {
	return &Manager{
		storage: storage,
		reader:  reader,
		writer:  writer,
		flight:  xsync.NewSingleFlight[string](),
	}
}

// Save saves a snapshot metadata with the given time range
// This is the single entry point for saving snapshots
// Snapshot only stores time range information, actual data can be rebuilt from entries
// startTsSeq: the start time sequence (format: "ts_seqid")
// stopTsSeq: the stop time sequence (format: "ts_seqid")
// score: the Redis score for the snapshot (must match stopTsSeq)
func (m *Manager) Save(ctx context.Context, catalog string, startTsSeq, stopTsSeq index.TimeSeqID, snapData []byte) (string, error) {
	return m.flight.Do(fmt.Sprintf("%s_%s_%s", catalog, startTsSeq.String(), stopTsSeq.String()), func() (string, error) {
		return m.save(ctx, catalog, startTsSeq, stopTsSeq, snapData)
	})
}

func (m *Manager) save(ctx context.Context, catalog string, startTsSeq, stopTsSeq index.TimeSeqID, snapData []byte) (string, error) {
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
