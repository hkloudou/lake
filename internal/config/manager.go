package config

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hkloudou/lake/v2/internal/storage"
	"github.com/hkloudou/lake/v2/internal/xsync"
	"github.com/redis/go-redis/v9"
)

// Manager manages lake configuration stored in Redis
type Manager struct {
	rdb    *redis.Client
	flight xsync.SingleFlight[*Config]
}

// NewManager creates a new config manager
func NewManager(rdb *redis.Client) *Manager {
	return &Manager{
		rdb:    rdb,
		flight: xsync.NewSingleFlight[*Config](),
	}
}

// Config represents lake configuration stored in Redis
type Config struct {
	Name      string `json:"Name"`
	Storage   string `json:"Storage"`   // "oss" | "s3" | "local"
	Bucket    string `json:"Bucket"`    // Bucket name
	Endpoint  string `json:"Endpoint"`  // OSS/S3 endpoint
	AccessKey string `json:"AccessKey"` // Access key
	SecretKey string `json:"SecretKey"` // Secret key
	AESPwd    string `json:"AESPwd"`    // AES encryption password
	// Region    string `json:"Region"`    // AWS region (for S3)
}

// Load loads configuration from Redis using SingleFlight
// Key: "lake.setting"
func (m *Manager) Load(ctx context.Context) (*Config, error) {
	return m.flight.Do("lake.setting", func() (*Config, error) {
		return m.load(ctx)
	})
}

func (m *Manager) load(ctx context.Context) (*Config, error) {
	// Read config from Redis
	data, err := m.rdb.Get(ctx, "lake.setting").Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("lake.setting not found in Redis")
		}
		return nil, fmt.Errorf("failed to read config from Redis: %w", err)
	}

	// Parse JSON
	var cfg Config
	if err := json.Unmarshal([]byte(data), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &cfg, nil
}

// Save saves configuration to Redis
func (m *Manager) Save(ctx context.Context, cfg *Config) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := m.rdb.Set(ctx, "lake.setting", string(data), 0).Err(); err != nil {
		return fmt.Errorf("failed to save config to Redis: %w", err)
	}

	return nil
}

// CreateStorage creates a storage instance based on configuration
func (cfg *Config) CreateStorage() (storage.Storage, error) {
	switch cfg.Storage {
	case "memory", "":
		// Memory storage for testing
		return storage.NewMemoryStorage(), nil

	case "oss":
		// Create OSS storage with encryption
		return storage.NewOSSStorage(storage.OSSConfig{
			Endpoint:  cfg.Endpoint,
			Bucket:    cfg.Bucket,
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
			AESKey:    cfg.AESPwd,
			Internal:  false, // TODO: make this configurable
		})

	case "s3":
		// TODO: Implement S3 storage
		return nil, fmt.Errorf("S3 storage not implemented yet")

	case "local":
		// TODO: Implement local file storage
		return nil, fmt.Errorf("local storage not implemented yet")

	default:
		return nil, fmt.Errorf("unknown storage type: %s", cfg.Storage)
	}
}

// DefaultConfig returns a default configuration
// Note: This should not be used in production
// Always configure lake.setting in Redis before using
func DefaultConfig() *Config {
	return &Config{
		Name:    "lake-default",
		Storage: "oss",
		Bucket:  "",
	}
}
