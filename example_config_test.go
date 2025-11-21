package lake_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/hkloudou/lake/v2/internal/config"
	lake "github.com/hkloudou/lake/v2/pkg/client"
)

func TestConfigManagement(t *testing.T) {
	// Create client with default config
	client := lake.New(lake.Config{
		RedisAddr: "localhost:6379",
	})

	ctx := context.Background()

	// Update config in Redis
	cfg := &config.Config{
		Name:      "my-lake",
		Storage:   "memory",
		Bucket:    "my-bucket",
		AccessKey: "test-key",
		SecretKey: "test-secret",
		AESPwd:    "encryption-password",
	}

	err := client.UpdateConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("UpdateConfig failed: %v", err)
	}

	// Load config from Redis
	loaded, err := client.GetConfig(ctx)
	if err != nil {
		t.Fatalf("GetConfig failed: %v", err)
	}

	fmt.Printf("Loaded config: %+v\n", loaded)
}

func TestNewFromRedisConfig(t *testing.T) {
	// First, setup config in Redis
	setupClient := lake.New(lake.Config{
		RedisAddr: "localhost:6379",
	})

	ctx := context.Background()
	cfg := &config.Config{
		Name:    "test-lake",
		Storage: "memory",
	}

	if err := setupClient.UpdateConfig(ctx, cfg); err != nil {
		t.Fatalf("Setup config failed: %v", err)
	}

	// Now create client from Redis config
	client, err := lake.NewFromRedisConfig("localhost:6379")
	if err != nil {
		t.Fatalf("NewFromRedisConfig failed: %v", err)
	}

	// Verify config was loaded
	loaded, err := client.GetConfig(ctx)
	if err != nil {
		t.Fatalf("GetConfig failed: %v", err)
	}

	if loaded.Name != cfg.Name {
		t.Errorf("Name mismatch: got %s, want %s", loaded.Name, cfg.Name)
	}

	// Write and read data to verify client works
	err = client.Write(ctx, lake.WriteRequest{
		Catalog: "test",
		Field:   "data",
		Value:   map[string]any{"foo": "bar"},
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	result, err := client.Read(ctx, lake.ReadRequest{
		Catalog: "test",
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	fmt.Printf("Read result: %+v\n", result.Data)
}

