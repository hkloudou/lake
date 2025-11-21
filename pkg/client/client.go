package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/internal/merge"
	"github.com/hkloudou/lake/v2/internal/snapshot"
	"github.com/hkloudou/lake/v2/internal/storage"
	"github.com/redis/go-redis/v9"
)

// Client is the main interface for Lake v2
type Client struct {
	storage storage.Storage
	writer  *index.Writer
	reader  *index.Reader
	merger  *merge.Engine
	snapMgr *snapshot.Manager
}

// Config holds client configuration
type Config struct {
	RedisAddr string
	Storage   storage.Storage // If nil, uses MemoryStorage
}

// New creates a new Lake client
func New(cfg Config) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})

	stor := cfg.Storage
	if stor == nil {
		stor = storage.NewMemoryStorage()
	}

	writer := index.NewWriter(rdb)
	reader := index.NewReader(rdb)
	merger := merge.NewEngine()
	snapMgr := snapshot.NewManager(stor, reader, writer, merger)

	return &Client{
		storage: stor,
		writer:  writer,
		reader:  reader,
		merger:  merger,
		snapMgr: snapMgr,
	}
}

// WriteRequest represents a write request
type WriteRequest struct {
	Catalog   string // Catalog name
	Field     string // JSON path (e.g., "user.profile.name")
	Value     any    // Value to write
	Timestamp int64  // Optional: if 0, uses current time
}

// Write writes data to the catalog
func (c *Client) Write(ctx context.Context, req WriteRequest) error {
	// Generate UUID
	docUUID := uuid.New().String()

	// Use current time if not specified
	if req.Timestamp == 0 {
		req.Timestamp = time.Now().Unix()
	}

	// Marshal value to JSON
	data, err := json.Marshal(req.Value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	// Write to storage
	key := storage.MakeKey(req.Catalog, docUUID)
	if err := c.storage.Put(ctx, key, data); err != nil {
		return fmt.Errorf("failed to write to storage: %w", err)
	}

	// Add to index
	if err := c.writer.Add(ctx, req.Catalog, req.Field, docUUID, req.Timestamp); err != nil {
		return fmt.Errorf("failed to add to index: %w", err)
	}

	return nil
}

// ReadRequest represents a read request
type ReadRequest struct {
	Catalog      string // Catalog name
	GenerateSnap bool   // Whether to generate snapshot automatically
}

// ReadResult represents the read result
type ReadResult struct {
	Data     map[string]any     // Merged JSON data
	Snapshot *snapshot.Snapshot // Snapshot info (if generated or used)
	Entries  []index.ReadResult // Raw entries (for debugging)
}

// Read reads and merges data from the catalog
func (c *Client) Read(ctx context.Context, req ReadRequest) (*ReadResult, error) {
	// Try to get existing snapshot
	snap, err := c.snapMgr.GetLatest(ctx, req.Catalog, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	var baseData map[string]any
	var entries []index.ReadResult

	if snap != nil {
		// Start from snapshot
		baseData = snap.Data

		// Read incremental data since snapshot
		entries, err = c.reader.ReadSince(ctx, req.Catalog, snap.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to read incremental data: %w", err)
		}
	} else {
		// No snapshot, read all
		baseData = make(map[string]any)
		entries, err = c.reader.ReadAll(ctx, req.Catalog)
		if err != nil {
			return nil, fmt.Errorf("failed to read all data: %w", err)
		}
	}

	// Merge incremental data
	merged := baseData
	for _, entry := range entries {
		// Read JSON from storage
		key := storage.MakeKey(req.Catalog, entry.UUID)
		data, err := c.storage.Get(ctx, key)
		if err != nil {
			continue // Skip missing data
		}

		var value any
		if err := json.Unmarshal(data, &value); err != nil {
			continue // Skip invalid JSON
		}

		// Merge
		merged, err = c.merger.Merge(merged, entry.Field, value, merge.StrategySet)
		if err != nil {
			return nil, fmt.Errorf("failed to merge: %w", err)
		}
	}

	result := &ReadResult{
		Data:     merged,
		Snapshot: snap,
		Entries:  entries,
	}

	// Generate snapshot if requested
	if req.GenerateSnap && len(entries) > 0 {
		newSnap, err := c.snapMgr.Generate(ctx, req.Catalog)
		if err == nil {
			result.Snapshot = newSnap
		}
		// Ignore error, not critical
	}

	return result, nil
}
