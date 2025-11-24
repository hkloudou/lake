package lake_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/hkloudou/lake/v2"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/internal/trace"
)

func TestWriteWithTrace(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")

	// Create context with trace enabled
	ctx := trace.WithTrace(context.Background())

	catalog := "test_trace"

	// Write with trace
	_, err := client.Write(ctx, lake.WriteRequest{
		Catalog:   catalog,
		Field:     "user.name",
		Body:      []byte(`"Bob"`),
		MergeType: index.MergeTypeReplace,
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Get trace and print
	tr := trace.FromContext(ctx)
	fmt.Println(tr.Dump())

	// Access individual spans
	spans := tr.GetSpans()
	for _, span := range spans {
		t.Logf("Span: %s -> %v %+v", span.Name, span.Duration, span.Details)
	}

	t.Logf("Total time: %v", tr.Total())
}

func TestMultipleWritesWithTrace(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
	catalog := "test_multi_trace"

	// Write 3 times with trace
	for i := 0; i < 3; i++ {
		ctx := trace.WithTrace(context.Background())

		_, err := client.Write(ctx, lake.WriteRequest{
			Catalog:   catalog,
			Field:     fmt.Sprintf("field_%d", i),
			Body:      []byte(fmt.Sprintf(`"value_%d"`, i)),
			MergeType: index.MergeTypeReplace,
		})
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}

		tr := trace.FromContext(ctx)
		t.Logf("\n=== Write %d ===\n%s", i, tr.Dump())
	}
}

func TestWriteWithoutTrace(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")

	// Regular context without trace (no overhead)
	ctx := context.Background()

	_, err := client.Write(ctx, lake.WriteRequest{
		Catalog:   "test_no_trace",
		Field:     "data",
		Body:      []byte(`"test"`),
		MergeType: index.MergeTypeReplace,
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// No trace output (silent)
	tr := trace.FromContext(ctx)
	if tr.Dump() != "" {
		t.Error("Expected empty trace for context without trace")
	}

	t.Log("✓ Write without trace successful (no logging)")
}
