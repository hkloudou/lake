package lake_test

import (
	"context"
	"fmt"
	"testing"

	lake "github.com/hkloudou/lake/v2/pkg/client"
)

func TestBasicUsage(t *testing.T) {
	// Create client with in-memory storage (for testing)
	client := lake.New(lake.Config{
		RedisAddr: "localhost:6379",
		Storage:   nil, // Uses MemoryStorage
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

