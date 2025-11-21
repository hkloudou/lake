package lake_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/hkloudou/lake/v2"
	"github.com/hkloudou/lake/v2/internal/config"
	"github.com/hkloudou/lake/v2/internal/storage"
)

func TestBasicUsage(t *testing.T) {
	// For testing, provide storage directly via options
	client := lake.NewLake("redis://localhost:6379", func(opt *lake.Option) {
		opt.Storage = storage.NewMemoryStorage()
	})

	ctx := context.Background()

	// Write some data
	err := client.Write(ctx, lake.WriteRequest{
		Catalog: "users",
		Field:   "profile.name",
		Value:   map[string]any{"first": "John", "last": "Doe"},
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	err = client.Write(ctx, lake.WriteRequest{
		Catalog: "users",
		Field:   "profile.age",
		Value:   30,
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read data
	result, err := client.Read(ctx, lake.ReadRequest{
		Catalog:      "users",
		GenerateSnap: true,
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	fmt.Printf("Merged data: %+v\n", result.Data)
	fmt.Printf("Number of entries: %d\n", len(result.Entries))
	if result.Snapshot != nil {
		fmt.Printf("Snapshot UUID: %s\n", result.Snapshot.UUID)
	}
}

func TestWithCustomStorage(t *testing.T) {
	// Provide custom storage via options
	client := lake.NewLake("localhost:6379", func(opt *lake.Option) {
		opt.Storage = storage.NewMemoryStorage()
	})

	ctx := context.Background()

	err := client.Write(ctx, lake.WriteRequest{
		Catalog: "test",
		Field:   "data",
		Value:   "hello",
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestReadStorage(t *testing.T) {
	// Test with real Redis config
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")

	ctx := context.Background()

	// Get config to see what's loaded
	cfg, err := client.GetConfig(ctx)
	if err != nil {
		t.Skipf("Skipping test: config not found in Redis: %v", err)
		return
	}
	t.Logf("Loaded config: Name=%s, Storage=%s, Bucket=%s", cfg.Name, cfg.Storage, cfg.Bucket)

	catalog := "test-read"

	// Write some test data
	t.Log("Writing test data...")
	err = client.Write(ctx, lake.WriteRequest{
		Catalog: catalog,
		Field:   "user.name",
		Value:   "Alice",
	})
	if err != nil {
		t.Fatalf("Write 1 failed: %v", err)
	}

	err = client.Write(ctx, lake.WriteRequest{
		Catalog: catalog,
		Field:   "user.age",
		Value:   25,
	})
	if err != nil {
		t.Fatalf("Write 2 failed: %v", err)
	}

	err = client.Write(ctx, lake.WriteRequest{
		Catalog: catalog,
		Field:   "user.email",
		Value:   "alice@example.com",
	})
	if err != nil {
		t.Fatalf("Write 3 failed: %v", err)
	}

	// Now read the data
	t.Log("Reading data...")
	result, err := client.Read(ctx, lake.ReadRequest{
		Catalog:      catalog,
		GenerateSnap: false,
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	t.Logf("Read result:")
	t.Logf("  Entries count: %d", len(result.Entries))
	t.Logf("  Data: %+v", result.Data)
	t.Logf("  Snapshot: %v", result.Snapshot != nil)

	if len(result.Entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(result.Entries))
	}

	if len(result.Data) == 0 {
		t.Error("Expected data but got empty map")
	}

	// Verify merged data structure
	if user, ok := result.Data["user"].(map[string]any); ok {
		if name, ok := user["name"].(string); !ok || name != "Alice" {
			t.Errorf("Expected user.name=Alice, got %v", user["name"])
		}
		if age, ok := user["age"].(float64); !ok || age != 25 {
			t.Errorf("Expected user.age=25, got %v", user["age"])
		}
		if email, ok := user["email"].(string); !ok || email != "alice@example.com" {
			t.Errorf("Expected user.email=alice@example.com, got %v", user["email"])
		}
	} else {
		t.Error("Expected user object in data")
	}
}

func TestConfigRequired(t *testing.T) {
	// This test verifies that client fails without proper config
	client := lake.NewLake("redis://localhost:6379/15") // Empty test DB

	ctx := context.Background()

	// Try to write without config - should fail
	err := client.Write(ctx, lake.WriteRequest{
		Catalog: "test",
		Field:   "data",
		Value:   "value",
	})

	if err == nil {
		t.Error("Expected error when config is missing, got nil")
	} else {
		t.Logf("Correctly failed with: %v", err)
	}
}

func TestSetupConfig(t *testing.T) {
	// Helper test to setup config in Redis
	t.Skip("Manual test - run only when needed to setup config")

	_ = lake.NewLake("redis://lake-redis-master.cs:6379/2", func(opt *lake.Option) {
		opt.Storage = storage.NewMemoryStorage() // Temporary for setup
	})

	_ = context.Background()

	cfg := &config.Config{
		Name:      "cs-lake",
		Storage:   "oss",
		Bucket:    "cs-lake",
		Endpoint:  "oss-cn-hangzhou",
		AccessKey: "your-access-key",
		SecretKey: "your-secret-key",
	}

	// This would fail because UpdateConfig is commented out
	// err := client.UpdateConfig(ctx, cfg)
	// For now, set manually in Redis:
	// redis-cli SET lake.setting '{"Name":"cs-lake","Storage":"oss","Bucket":"cs-lake",...}'

	t.Logf("Config to set: %+v", cfg)
	t.Log("Run manually: redis-cli SET lake.setting '{...}'")
}
