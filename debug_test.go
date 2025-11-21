package lake

import (
	"context"
	"testing"
)

func TestDebugKeys(t *testing.T) {
	client := NewLake("redis://lake-redis-master.cs:6379/2")
	ctx := context.Background()

	catalog := "debug-test"

	// Write data
	t.Log("Writing test data...")
	err := client.Write(ctx, WriteRequest{
		Catalog: catalog,
		Field:   "test.field",
		Value:   "test-value",
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Check what's in Redis
	t.Log("Checking Redis keys...")
	keys, err := client.rdb.Keys(ctx, "*debug-test*").Result()
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}

	t.Logf("Found keys: %v", keys)

	if len(keys) == 0 {
		// Try without catalog name
		allKeys, _ := client.rdb.Keys(ctx, "*").Result()
		t.Logf("All keys in Redis: %v", allKeys)
	}

	for _, key := range keys {
		members, err := client.rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err == nil {
			t.Logf("Key %s has %d members:", key, len(members))
			for _, m := range members {
				t.Logf("  - score=%.0f, member=%s", m.Score, m.Member)
			}
		}
	}

	// Now try to read
	t.Log("Reading data...")
	result, err := client.Read(ctx, ReadRequest{
		Catalog:      catalog,
		GenerateSnap: false,
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	t.Logf("Read result: %d entries, data=%+v", len(result.Entries), result.Data)
}
