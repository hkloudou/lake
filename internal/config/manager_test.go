package config

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestConfigManager(t *testing.T) {
	// Use a test Redis instance
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // Use test database
	})

	ctx := context.Background()

	// Clean up
	defer rdb.Del(ctx, "lake.setting")

	mgr := NewManager(rdb)

	// Save config
	cfg := &Config{
		Name:      "test-lake",
		Storage:   "memory",
		Bucket:    "test-bucket",
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}

	if err := mgr.Save(ctx, cfg); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Load config
	loaded, err := mgr.Load(ctx)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if loaded.Name != cfg.Name {
		t.Errorf("Name mismatch: got %s, want %s", loaded.Name, cfg.Name)
	}

	if loaded.Storage != cfg.Storage {
		t.Errorf("Storage mismatch: got %s, want %s", loaded.Storage, cfg.Storage)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Name != "lake" {
		t.Errorf("Default name should be 'lake', got %s", cfg.Name)
	}

	if cfg.Storage != "memory" {
		t.Errorf("Default storage should be 'memory', got %s", cfg.Storage)
	}
}

func TestCreateStorage(t *testing.T) {
	cfg := &Config{Storage: "memory"}

	stor, err := cfg.CreateStorage()
	if err != nil {
		t.Fatalf("CreateStorage failed: %v", err)
	}

	if stor == nil {
		t.Error("Storage should not be nil")
	}
}
