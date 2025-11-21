package client

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v2/internal/config"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/internal/merge"
	"github.com/hkloudou/lake/v2/internal/snapshot"
	"github.com/redis/go-redis/v9"
)

// NewFromRedisConfig creates a new Lake client from Redis configuration
// Loads config from "lake.setting" using SingleFlight
// This is the recommended way to initialize the client in production
func NewFromRedisConfig(redisAddr string) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	configMgr := config.NewManager(rdb)

	// Load config from Redis (uses SingleFlight to prevent duplicate loads)
	ctx := context.Background()
	lakeCfg, err := configMgr.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load config from Redis: %w", err)
	}

	// Create storage from config
	stor, err := lakeCfg.CreateStorage()
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	writer := index.NewWriter(rdb)
	reader := index.NewReader(rdb)
	merger := merge.NewEngine()
	snapMgr := snapshot.NewManager(stor, reader, writer, merger)

	return &Client{
		storage:   stor,
		writer:    writer,
		reader:    reader,
		merger:    merger,
		snapMgr:   snapMgr,
		rdb:       rdb,
		configMgr: configMgr,
	}, nil
}

// GetConfig returns the current config from Redis (uses SingleFlight)
func (c *Client) GetConfig(ctx context.Context) (*config.Config, error) {
	if c.configMgr == nil {
		return nil, fmt.Errorf("config manager not initialized")
	}
	return c.configMgr.Load(ctx)
}

// UpdateConfig updates the config in Redis
func (c *Client) UpdateConfig(ctx context.Context, cfg *config.Config) error {
	if c.configMgr == nil {
		return fmt.Errorf("config manager not initialized")
	}
	return c.configMgr.Save(ctx, cfg)
}
