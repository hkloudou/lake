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
	_, err := client.Write(ctx, lake.WriteRequest{
		Catalog:   "users",
		Field:     "profile.name",
		Value:     map[string]any{"first": "John", "last": "Doe"},
		MergeType: 0, // Replace
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_, err = client.Write(ctx, lake.WriteRequest{
		Catalog:   "users",
		Field:     "profile.age",
		Value:     30,
		MergeType: 0, // Replace
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

	_, err := client.Write(ctx, lake.WriteRequest{
		Catalog:   "test",
		Field:     "data",
		Value:     "hello",
		MergeType: 0, // Replace
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWriteData(t *testing.T) {
	// Test writing data with real Redis config
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")

	ctx := context.Background()

	catalog := "test"
	var err error

	// Write test data
	t.Log("Writing test data...")

	_, err = client.Write(ctx, lake.WriteRequest{
		Catalog:   catalog,
		Field:     "user.name",
		Value:     "Alice2",
		MergeType: 0, // Replace
	})
	if err != nil {
		t.Fatalf("Write user.name failed: %v", err)
	}
	t.Log("✓ Wrote user.name")

	// _, err = client.Write(ctx, lake.WriteRequest{
	// 	Catalog:   catalog,
	// 	Field:     "user.age",
	// 	Value:     25,
	// 	MergeType: 0, // Replace
	// })
	// if err != nil {
	// 	t.Fatalf("Write user.age failed: %v", err)
	// }
	// t.Log("✓ Wrote user.age")

	// _, err = client.Write(ctx, lake.WriteRequest{
	// 	Catalog:   catalog,
	// 	Field:     "user.email",
	// 	Value:     "alice@example.com",
	// 	MergeType: 0, // Replace
	// })
	// if err != nil {
	// 	t.Fatalf("Write user.email failed: %v", err)
	// }
	// t.Log("✓ Wrote user.email")

	t.Log("All writes completed successfully!")
}

func TestReadStorage(t *testing.T) {
	// Test reading data with real Redis config
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")

	ctx := context.Background()

	catalog := "test"

	// Read the data
	t.Log("Reading data...")
	result, err := client.Read(ctx, lake.ReadRequest{
		Catalog:      catalog,
		GenerateSnap: true,
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	t.Logf("Read result:")
	t.Logf("  Entries count: %d", len(result.Entries))
	t.Logf("  Data: %+v", result.Data)
	// t.Logf("  Snapshot: %v", result.Snapshot != nil)
	if result.Snapshot != nil {
		t.Logf("  Snapshot UUID: %s", result.Snapshot.UUID)
		t.Logf("  Snapshot Timestamp: %d", result.Snapshot.Timestamp)
		t.Logf("  Snapshot Data: %+v", result.Snapshot.Data)
	} else {
		t.Log("No snapshot found")
	}

	if len(result.Entries) == 0 {
		t.Log("No entries found - catalog may be empty")
		return
	}

	if len(result.Data) == 0 {
		t.Error("Expected data but got empty map")
		return
	}

	// Verify merged data structure if user data exists
	if user, ok := result.Data["user"].(map[string]any); ok {
		t.Logf("User data found: %+v", user)

		if name, ok := user["name"].(string); ok {
			t.Logf("  ✓ user.name = %s", name)
		}
		if age, ok := user["age"].(float64); ok {
			t.Logf("  ✓ user.age = %.0f", age)
		}
		if email, ok := user["email"].(string); ok {
			t.Logf("  ✓ user.email = %s", email)
		}
	}
}

func TestConfigRequired(t *testing.T) {
	// This test verifies that client fails without proper config
	client := lake.NewLake("redis://localhost:6379/15") // Empty test DB

	ctx := context.Background()

	// Try to write without config - should fail
	_, err := client.Write(ctx, lake.WriteRequest{
		Catalog:   "test",
		Field:     "data",
		Value:     "value",
		MergeType: 0, // Replace
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
