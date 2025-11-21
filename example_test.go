package lake_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/hkloudou/lake/v2"
)

func TestBasicUsage(t *testing.T) {
	// Create client - no error returned, initialization is lazy
	client := lake.NewLake("redis://localhost:6379")

	ctx := context.Background()

	// Write some data (config loaded automatically on first operation)
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

func TestWriteStorage(t *testing.T) {
	// Can provide custom storage via options
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2", func(opt *lake.Option) {
		// opt.Storage = myCustomStorage
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

	fmt.Println("Write successful")
}

func TestReadStorage(t *testing.T) {
	// Test reading from real Redis
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")

	ctx := context.Background()

	// First, get config to see what's loaded
	cfg, err := client.GetConfig(ctx)
	if err != nil {
		t.Logf("Config load error (will use defaults): %v", err)
	} else {
		t.Logf("Loaded config: Name=%s, Storage=%s, Bucket=%s", cfg.Name, cfg.Storage, cfg.Bucket)
	}

	catalog := "test-read"

	// Write some test data first
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
