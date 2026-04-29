package config

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/storage"
	"github.com/hkloudou/lake/v3/internal/xsync"
	"github.com/redis/go-redis/v9"
)

// settingKey is the Redis key holding the deployment configuration JSON.
const settingKey = "lake.setting"

// Manager loads and persists Lake configuration from Redis (key
// lake.setting). Concurrent loads are deduped by SingleFlight.
type Manager struct {
	rdb    *redis.Client
	flight xsync.SingleFlight[*Config]
}

func NewManager(rdb *redis.Client) *Manager {
	return &Manager{rdb: rdb, flight: xsync.NewSingleFlight[*Config]()}
}

// Config is the deployment-level configuration stored in Redis.
type Config struct {
	Name    string `json:"Name"`
	Storage string `json:"Storage"` // "memory" | "oss" | "file"
	AESPwd  string `json:"AESPwd"`

	Bucket    string `json:"Bucket"`    // OSS
	Endpoint  string `json:"Endpoint"`  // OSS
	AccessKey string `json:"AccessKey"` // OSS
	SecretKey string `json:"SecretKey"` // OSS

	BasePath string `json:"BasePath"` // file
}

func (m *Manager) Load(ctx context.Context) (*Config, error) {
	return m.flight.Do(settingKey, func() (*Config, error) {
		raw, err := m.rdb.Get(ctx, settingKey).Result()
		if err == redis.Nil {
			return nil, fmt.Errorf("%s not found in Redis", settingKey)
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", settingKey, err)
		}
		var cfg Config
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			return nil, fmt.Errorf("parse %s: %w", settingKey, err)
		}
		return &cfg, nil
	})
}

func (m *Manager) Save(ctx context.Context, cfg *Config) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := m.rdb.Set(ctx, settingKey, string(data), 0).Err(); err != nil {
		return fmt.Errorf("save %s: %w", settingKey, err)
	}
	return nil
}

// CreateStorage instantiates the configured backend.
func (cfg *Config) CreateStorage() (storage.Storage, error) {
	switch cfg.Storage {
	case "memory", "":
		return storage.NewMemoryStorage(cfg.Name), nil
	case "oss":
		return storage.NewOSSStorage(storage.OSSConfig{
			Name:      cfg.Name,
			Endpoint:  cfg.Endpoint,
			Bucket:    cfg.Bucket,
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
			AESKey:    cfg.AESPwd,
		})
	case "file":
		return storage.NewFileStorage(storage.FileConfig{
			Name:     cfg.Name,
			BasePath: cfg.BasePath,
			AESKey:   cfg.AESPwd,
		})
	default:
		return nil, fmt.Errorf("unknown storage type: %s", cfg.Storage)
	}
}

// DefaultConfig returns a placeholder configuration. Not for production —
// always populate lake.setting in Redis.
func DefaultConfig() *Config {
	return &Config{Name: "lake", Storage: "memory"}
}
