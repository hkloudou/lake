package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/internal/merge"
	"github.com/hkloudou/lake/v2/internal/storage"
	"github.com/hkloudou/lake/v2/internal/xsync"
)

// Manager handles snapshot generation and reading
type Manager struct {
	storage storage.Storage
	reader  *index.Reader
	writer  *index.Writer
	merger  *merge.Engine
	flight  xsync.SingleFlight[*Snapshot]
}

// NewManager creates a new snapshot manager
func NewManager(
	storage storage.Storage,
	reader *index.Reader,
	writer *index.Writer,
	merger *merge.Engine,
) *Manager {
	return &Manager{
		storage: storage,
		reader:  reader,
		writer:  writer,
		merger:  merger,
		flight:  xsync.NewSingleFlight[*Snapshot](),
	}
}

// Snapshot represents a snapshot
type Snapshot struct {
	UUID      string         `json:"uuid"`
	Catalog   string         `json:"catalog"`
	Timestamp int64          `json:"timestamp"`
	Data      map[string]any `json:"data"`
}

// Generate generates a snapshot for the catalog
// Uses SingleFlight to prevent duplicate snapshot generation
func (m *Manager) Generate(ctx context.Context, catalog string) (*Snapshot, error) {
	return m.flight.Do(catalog, func() (*Snapshot, error) {
		return m.generate(ctx, catalog)
	})
}

func (m *Manager) generate(ctx context.Context, catalog string) (*Snapshot, error) {
	// Read all entries from index
	entries, err := m.reader.ReadAll(ctx, catalog)
	if err != nil {
		return nil, fmt.Errorf("failed to read index: %w", err)
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("no data to snapshot")
	}

	// Merge all data
	merged := make(map[string]any)
	for _, entry := range entries {
		// Read JSON from storage
		key := storage.MakeKey(catalog, entry.UUID)
		data, err := m.storage.Get(ctx, key)
		if err != nil {
			continue // Skip missing data
		}

		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			continue // Skip invalid JSON
		}

		// Merge with strategy "set" (always overwrite)
		merged, err = m.merger.Merge(merged, entry.Field, value, merge.StrategySet)
		if err != nil {
			return nil, fmt.Errorf("failed to merge: %w", err)
		}
	}

	// Create snapshot
	snapUUID := uuid.New().String()
	lastTimestamp := entries[len(entries)-1].Timestamp

	snap := &Snapshot{
		UUID:      snapUUID,
		Catalog:   catalog,
		Timestamp: lastTimestamp,
		Data:      merged,
	}

	// Save snapshot to storage
	snapData, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	snapKey := storage.MakeKey(catalog, snapUUID)
	if err := m.storage.Put(ctx, snapKey, snapData); err != nil {
		return nil, fmt.Errorf("failed to save snapshot: %w", err)
	}

	// Add snapshot to index
	if err := m.writer.AddSnap(ctx, catalog, snapUUID, lastTimestamp); err != nil {
		return nil, fmt.Errorf("failed to add snapshot to index: %w", err)
	}

	return snap, nil
}

// GetLatest gets the latest snapshot or generates one if needed
func (m *Manager) GetLatest(ctx context.Context, catalog string, generateIfMissing bool) (*Snapshot, error) {
	// Check for existing snapshot
	snapInfo, err := m.reader.GetLatestSnap(ctx, catalog)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	// If snapshot exists, load it
	if snapInfo != nil {
		snap, err := m.loadSnapshot(ctx, catalog, snapInfo.UUID)
		if err == nil {
			return snap, nil
		}
		// If loading fails, fall through to generate new one
	}

	// Generate new snapshot if needed
	if generateIfMissing {
		return m.Generate(ctx, catalog)
	}

	return nil, nil
}

func (m *Manager) loadSnapshot(ctx context.Context, catalog, snapUUID string) (*Snapshot, error) {
	key := storage.MakeKey(catalog, snapUUID)
	data, err := m.storage.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}

	return &snap, nil
}

// ShouldGenerate checks if a snapshot should be generated based on strategy
func ShouldGenerate(lastSnapTime time.Time, entryCount int, strategy GenerationStrategy) bool {
	switch strategy {
	case StrategyAlways:
		return true
	case StrategyNever:
		return false
	case StrategyAuto:
		// Generate if:
		// 1. No snapshot exists (lastSnapTime is zero)
		// 2. Last snapshot is older than 1 hour and has more than 100 entries
		if lastSnapTime.IsZero() {
			return entryCount > 10
		}
		age := time.Since(lastSnapTime)
		return age > time.Hour && entryCount > 100
	default:
		return false
	}
}

// GenerationStrategy defines when to generate snapshots
type GenerationStrategy int

const (
	StrategyAuto   GenerationStrategy = iota // Auto-generate based on heuristics
	StrategyAlways                           // Always generate
	StrategyNever                            // Never generate
)
