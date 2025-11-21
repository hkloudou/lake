package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/internal/storage"
	"github.com/hkloudou/lake/v2/internal/xsync"
)

// Manager handles snapshot saving and reading
type Manager struct {
	storage storage.Storage
	reader  *index.Reader
	writer  *index.Writer
	flight  xsync.SingleFlight[*Snapshot]
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

// Save saves a snapshot with the given data
// This is the single entry point for saving snapshots
// Data merging should be done by the caller (Client.Read)
func (m *Manager) Save(ctx context.Context, catalog string, data map[string]any, timestamp int64) (*Snapshot, error) {
	return m.flight.Do(catalog, func() (*Snapshot, error) {
		return m.save(ctx, catalog, data, timestamp)
	})
}

func (m *Manager) save(ctx context.Context, catalog string, data map[string]any, timestamp int64) (*Snapshot, error) {
	// Create snapshot
	snapUUID := uuid.New().String()

	snap := &Snapshot{
		UUID:      snapUUID,
		Catalog:   catalog,
		Timestamp: timestamp,
		Data:      data,
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
	if err := m.writer.AddSnap(ctx, catalog, snapUUID, timestamp); err != nil {
		return nil, fmt.Errorf("failed to add snapshot to index: %w", err)
	}

	return snap, nil
}

// GetLatest gets the latest snapshot
func (m *Manager) GetLatest(ctx context.Context, catalog string, _ bool) (*Snapshot, error) {
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
	}

	// No snapshot found
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
