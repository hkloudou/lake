package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hkloudou/lake/v2/internal/cache"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/internal/storage"
	"github.com/hkloudou/lake/v2/internal/xsync"
)

// Manager handles snapshot saving and reading
type Manager struct {
	storage storage.Storage
	reader  *index.Reader
	writer  *index.Writer
	cache   cache.Cache
	flight  xsync.SingleFlight[string]
}

// NewManager creates a new snapshot manager
func NewManager(
	storage storage.Storage,
	reader *index.Reader,
	writer *index.Writer,
	cacheProvider cache.Cache,
) *Manager {
	return &Manager{
		storage: storage,
		reader:  reader,
		writer:  writer,
		cache:   cacheProvider,
		flight:  xsync.NewSingleFlight[string](),
	}
}

// Snapshot represents a snapshot (time range only, no data)
type Snapshot struct {
	// UUID       string          `json:"uuid"`
	Catalog    string          `json:"catalog"`
	StartTsSeq index.TimeSeqID `json:"start_ts_seq"` // Start time sequence
	StopTsSeq  index.TimeSeqID `json:"stop_ts_seq"`  // Stop time sequence
	Score      float64         `json:"score"`        // For backward compatibility (score)
}

// Save saves a snapshot metadata with the given time range
// This is the single entry point for saving snapshots
// Snapshot only stores time range information, actual data can be rebuilt from entries
// startTsSeq: the start time sequence (format: "ts_seqid")
// stopTsSeq: the stop time sequence (format: "ts_seqid")
// score: the Redis score for the snapshot (must match stopTsSeq)
func (m *Manager) Save(ctx context.Context, catalog string, startTsSeq, stopTsSeq index.TimeSeqID, snapData []byte) (string, error) {
	return m.flight.Do(catalog, func() (string, error) {
		return m.save(ctx, catalog, startTsSeq, stopTsSeq, snapData)
	})
}

func (m *Manager) save(ctx context.Context, catalog string, startTsSeq, stopTsSeq index.TimeSeqID, snapData []byte) (string, error) {
	// Validate: stopTsSeq and score must match
	// tsSeq, err := index.ParseTimeSeqID(stopTsSeq)
	// if err != nil {
	// 	return nil, fmt.Errorf("invalid stopTsSeq format: %w", err)
	// }
	// expectedScore := stopTsSeq.Score()
	// if score != expectedScore {
	// 	return nil, fmt.Errorf("score mismatch: got %f, expected %f from stopTsSeq %s",
	// 		score, expectedScore, stopTsSeq)
	// }

	// Create snapshot metadata (no data stored)
	// snapUUID := uuid.New().String()

	// snap := &Snapshot{
	// 	// UUID:       snapUUID,
	// 	Catalog:    catalog,
	// 	StartTsSeq: startTsSeq,
	// 	StopTsSeq:  stopTsSeq,
	// 	Score:      score, // For backward compatibility
	// }

	// Save snapshot metadata to storage
	// Filename: catalog/snap/startTsSeq~stopTsSeq.snap
	// snapData, err := json.Marshal(snap)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to marshal snapshot: %w", err)
	// }

	snapKey := storage.MakeSnapKey(catalog, startTsSeq, stopTsSeq)
	if err := m.storage.Put(ctx, snapKey, snapData); err != nil {
		return "", fmt.Errorf("failed to save snapshot: %w", err)
	}

	// Add snapshot to index with time range
	if err := m.writer.AddSnap(ctx, catalog, startTsSeq, stopTsSeq); err != nil {
		return "", fmt.Errorf("failed to add snapshot to index: %w", err)
	}

	return snapKey, nil
}

// GetLatest gets the latest snapshot metadata
// Returns the snapshot along with its time range information
func (m *Manager) GetLatest(ctx context.Context, catalog string, _ bool) (*Snapshot, error) {
	// Check for existing snapshot
	snapInfo, err := m.reader.GetLatestSnap(ctx, catalog)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	// If snapshot exists, load metadata
	if snapInfo != nil {
		// snap, err := m.loadSnapshot(ctx, catalog, snapInfo.StartTsSeq, snapInfo.StopTsSeq)
		// if err == nil {
		// 	return snap, nil
		// }
		// If load fails, return the info we have from Redis
		return &Snapshot{
			StartTsSeq: snapInfo.StartTsSeq,
			StopTsSeq:  snapInfo.StopTsSeq,
			// Score:      snapInfo.Score,
		}, nil
	}

	// No snapshot found
	return nil, nil
}

// loadSnapshot loads snapshot using time range with cache support
// filename: catalog/{startTsSeq}~{stopTsSeq}.snap
func (m *Manager) loadSnapshot(ctx context.Context, catalog string, startTsSeq, stopTsSeq index.TimeSeqID) (*Snapshot, error) {
	key := storage.MakeSnapKey(catalog, startTsSeq, stopTsSeq)

	// Use cache if available
	obj, err := m.cache.Take(key, func() (any, error) {
		// Cache miss: load from storage
		data, err := m.storage.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		var snap Snapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return nil, err
		}

		return &snap, nil
	})

	if err != nil {
		return nil, err
	}

	snap, ok := obj.(*Snapshot)
	if !ok {
		// Type assertion failed, reload from storage
		data, err := m.storage.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		var snapObj Snapshot
		if err := json.Unmarshal(data, &snapObj); err != nil {
			return nil, err
		}
		return &snapObj, nil
	}

	return snap, nil
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
