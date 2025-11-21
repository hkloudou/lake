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

func TestWithCustomStorage(t *testing.T) {
	// Can provide custom storage via options
	client := lake.NewLake("localhost:6379", func(opt *lake.Option) {
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
}
