package lake

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hkloudou/lake/v2/internal/config"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/internal/merge"
	"github.com/hkloudou/lake/v2/internal/snapshot"
	"github.com/hkloudou/lake/v2/internal/storage"
	"github.com/redis/go-redis/v9"
)

// Client is the main interface for Lake v2
type Client struct {
	rdb       *redis.Client
	writer    *index.Writer
	reader    *index.Reader
	merger    *merge.Engine
	configMgr *config.Manager

	// Lazy-loaded components
	mu      sync.RWMutex
	storage storage.Storage
	snapMgr *snapshot.Manager
	config  *config.Config
}

// Option is a function that configures the client
type Option struct {
	Storage storage.Storage
}

// NewLake creates a new Lake client with the given Redis URL
// Config is loaded lazily on first operation
func NewLake(metaUrl string, opts ...func(*Option)) *Client {
	// Parse Redis URL
	redisOpt, err := redis.ParseURL(metaUrl)
	if err != nil {
		// Fallback to treating it as an address
		redisOpt = &redis.Options{
			Addr: metaUrl,
		}
	}

	rdb := redis.NewClient(redisOpt)

	// Apply options
	option := &Option{}
	for _, opt := range opts {
		opt(option)
	}

	writer := index.NewWriter(rdb)
	reader := index.NewReader(rdb)
	merger := merge.NewEngine()
	configMgr := config.NewManager(rdb)

	client := &Client{
		rdb:       rdb,
		writer:    writer,
		reader:    reader,
		merger:    merger,
		configMgr: configMgr,
		storage:   option.Storage, // May be nil, will be loaded lazily
	}

	return client
}

// ensureInitialized ensures storage and snapMgr are initialized
// Loads config from Redis if not already loaded
func (c *Client) ensureInitialized(ctx context.Context) error {
	c.mu.RLock()
	if c.storage != nil && c.snapMgr != nil {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if c.storage != nil && c.snapMgr != nil {
		return nil
	}

	// Load config and initialize storage if not provided
	if c.storage == nil {
		// Load config from Redis if not already loaded
		if c.config == nil {
			cfg, err := c.configMgr.Load(ctx)
			if err != nil {
				return fmt.Errorf("failed to load config from Redis (lake.setting): %w", err)
			}
			c.config = cfg
		}

		// Create storage from config - must succeed, no fallback
		stor, err := c.config.CreateStorage()
		if err != nil {
			return fmt.Errorf("failed to create %s storage: %w", c.config.Storage, err)
		}
		c.storage = stor

		// Set index prefix based on config: Storage:Name
		prefix := fmt.Sprintf("%s:%s", c.config.Storage, c.config.Name)
		c.writer.SetPrefix(prefix)
		c.reader.SetPrefix(prefix)
	}

	// Initialize snapshot manager
	if c.snapMgr == nil {
		c.snapMgr = snapshot.NewManager(c.storage, c.reader, c.writer)
	}

	return nil
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
	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}

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
	if c.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
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

// mergeEntries merges entries into baseData
// This is the single source of truth for data merging
func (c *Client) mergeEntries(ctx context.Context, catalog string, baseData map[string]any, entries []index.ReadResult) (map[string]any, error) {
	merged := baseData
	for _, entry := range entries {
		// Read JSON from storage
		key := storage.MakeKey(catalog, entry.UUID)
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
	return merged, nil
}

// Read reads and merges data from the catalog
func (c *Client) Read(ctx context.Context, req ReadRequest) (*ReadResult, error) {
	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	// Try to get existing snapshot
	snap, err := c.snapMgr.GetLatest(ctx, req.Catalog, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	var baseData map[string]any
	var entries []index.ReadResult
	var allEntries []index.ReadResult // For snapshot generation

	if snap != nil {
		// Start from snapshot
		baseData = snap.Data

		// Read incremental data since snapshot
		entries, err = c.reader.ReadSince(ctx, req.Catalog, snap.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to read incremental data: %w", err)
		}
		allEntries = entries
	} else {
		// No snapshot, read all
		baseData = make(map[string]any)
		allEntries, err = c.reader.ReadAll(ctx, req.Catalog)
		if err != nil {
			return nil, fmt.Errorf("failed to read all data: %w", err)
		}
		entries = allEntries
	}

	// Merge data (single source of truth)
	merged, err := c.mergeEntries(ctx, req.Catalog, baseData, entries)
	if err != nil {
		return nil, err
	}

	result := &ReadResult{
		Data:     merged,
		Snapshot: snap,
		Entries:  allEntries, // Return all entries for debugging
	}

	// Generate snapshot if requested
	if req.GenerateSnap && len(allEntries) > 0 {
		// Save snapshot with merged data
		lastTimestamp := allEntries[len(allEntries)-1].Timestamp
		newSnap, err := c.snapMgr.Save(ctx, req.Catalog, merged, lastTimestamp)
		if err == nil {
			result.Snapshot = newSnap
		}
		// Ignore error, not critical
	}

	return result, nil
}

// GetConfig returns the current config (loads from Redis if needed)
func (c *Client) GetConfig(ctx context.Context) (*config.Config, error) {
	c.mu.RLock()
	if c.config != nil {
		cfg := c.config
		c.mu.RUnlock()
		return cfg, nil
	}
	c.mu.RUnlock()

	// Load config
	cfg, err := c.configMgr.Load(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.config = cfg
	c.mu.Unlock()

	return cfg, nil
}

// UpdateConfig updates the config in Redis
// DEPRECATED: Temporarily disabled. DO NOT DELETE this code.
// Will be re-enabled after testing storage reinitialization logic.
/*
func (c *Client) UpdateConfig(ctx context.Context, cfg *config.Config) error {
	if err := c.configMgr.Save(ctx, cfg); err != nil {
		return err
	}

	// Update cached config
	c.mu.Lock()
	c.config = cfg
	// TODO: Reinitialize storage based on new config
	c.mu.Unlock()

	return nil
}
*/
