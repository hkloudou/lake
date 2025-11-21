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
		t.Logf("Loaded config: %+v", cfg)
	}

	// Try to read
	result, err := client.Read(ctx, lake.ReadRequest{
		Catalog:      "test",
		GenerateSnap: false,
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	t.Logf("Read result: Data=%+v, Entries count=%d", result.Data, len(result.Entries))

	if len(result.Data) == 0 {
		t.Log("No data found - this is expected if catalog is empty")
	}
}
