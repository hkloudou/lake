package lake_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/hkloudou/lake/v2"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/internal/storage"
)

func TestBasicUsage(t *testing.T) {
	// For testing, provide storage directly via options
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2", func(opt *lake.Option) {
		opt.Storage = storage.NewMemoryStorage("test")
	})

	ctx := context.Background()

	// Write some data (Body is raw JSON)
	_, err := client.Write(ctx, lake.WriteRequest{
		Catalog:   "users",
		Field:     "profile.name",
		Body:      []byte(`"Alice"`), // JSON string
		MergeType: index.MergeTypeReplace,
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_, err = client.Write(ctx, lake.WriteRequest{
		Catalog:   "users",
		Field:     "profile.age",
		Body:      []byte(`30`), // JSON number
		MergeType: index.MergeTypeReplace,
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	t.Log("✓ Basic write operations successful")
	result, err := client.List(ctx, "users")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	t.Logf("List result: %+v", result.Dump())
	data, err := lake.ReadMap(ctx, result)
	if err != nil {
		t.Fatalf("ReadMap failed: %v", err)
	}
	t.Logf("Data: %+v", data)
}

func TestWriteRFC6902(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
	ctx := context.Background()
	catalog := "test_rfc6902"

	// Test 1: RFC6902 at root level
	t.Run("root level patch", func(t *testing.T) {
		patchOps := []byte(`[
			{ "op": "add", "path": "/a/b/c", "value": {"name": "John", "age": 30} },
			{ "op": "replace", "path": "/a/b/c", "value": 42 },
			{ "op": "move", "from": "/a/b/c", "path": "/a/b/d" },
			{ "op": "copy", "from": "/a/b/d", "path": "/a/b/e" }
		]`)

		_, err := client.Write(ctx, lake.WriteRequest{
			Catalog:   catalog,
			Field:     "", // Empty field means root document
			Body:      patchOps,
			MergeType: index.MergeTypeRFC6902,
		})
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		t.Log("✓ RFC6902 patch to root document successful")
	})

	// Test 2: RFC6902 at field level
	t.Run("field level patch", func(t *testing.T) {
		patchOpsField := []byte(`[
			{ "op": "add", "path": "/x", "value": {"name": "Alice"} },
			{ "op": "add", "path": "/y", "value": 123 }
		]`)

		_, err := client.Write(ctx, lake.WriteRequest{
			Catalog:   catalog,
			Field:     "profile", // Patch applies to "profile" field only
			Body:      patchOpsField,
			MergeType: index.MergeTypeRFC6902,
		})
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		t.Log("✓ RFC6902 patch to 'profile' field successful")
	})

	// Verify the data
	result, err := client.List(ctx, catalog)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	data, err := lake.ReadMap(ctx, result)
	if err != nil {
		t.Fatalf("ReadMap failed: %v", err)
	}

	t.Logf("Final data: %+v", data)
}

func TestWriteData(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
	ctx := context.Background()
	catalog := "test_write"

	// Test different merge types
	t.Run("replace", func(t *testing.T) {
		_, err := client.Write(ctx, lake.WriteRequest{
			Catalog:   catalog,
			Field:     "user.name",
			Body:      []byte(`"Alice"`), // JSON string
			MergeType: index.MergeTypeReplace,
		})
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		t.Log("✓ Replace successful")
	})

	t.Run("rfc7396 merge", func(t *testing.T) {
		_, err := client.Write(ctx, lake.WriteRequest{
			Catalog:   catalog,
			Field:     "user",
			Body:      []byte(`{"age": 30, "city": "NYC"}`), // JSON object
			MergeType: index.MergeTypeRFC7396,
		})
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		t.Log("✓ RFC7396 merge successful")
	})

	t.Log("All write operations completed successfully!")
}

func TestListAndRead(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
	ctx := context.Background()
	catalog := "test_write"
	// List catalog entries
	result, err := client.List(ctx, catalog)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	t.Log("Catalog entries:")
	fmt.Println(result.Dump())

	// Read merged data
	data, err := lake.ReadMap(ctx, result)
	if err != nil {
		t.Fatalf("ReadMap failed: %v", err)
	}

	t.Logf("Merged data: %+v", data)
}
