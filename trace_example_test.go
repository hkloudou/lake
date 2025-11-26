package lake_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hkloudou/lake/v2"
	"github.com/hkloudou/lake/v2/internal/trace"
)

//	func TestReadWithTrace(t *testing.T) {
//		client := lake.NewLake("redis://lake-redis-master.cs:6379/2", lake.WithRedisCache("redis://lake-redis-master.cs:6379/2", 1*time.Hour))
//		// Create context with trace enabled (operation name auto-detected or specified)
//		ctx := trace.WithTrace(context.Background())
//		for i := 0; i < 5; i++ {
//			go func() {
//				t.Log(lake.ReadString(ctx, client.List(ctx, "test_trace")))
//			}()
//		}
//		// Get trace and print
//		tr := trace.FromContext(ctx)
//		time.Sleep(2 * time.Second)
//		fmt.Println(tr.Dump())
//		t.Logf("Total time: %v", tr.Total())
//	}
func TestReadSimple(t *testing.T) {

	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
	// Create context with trace enabled (operation name auto-detected or specified)
	ctx := trace.WithTrace(context.Background())
	// for i := 0; i < 5; i++ {
	// 	go func() {
	// t.Log()
	// return
	// client.List(ctx, "test_trace")
	t.Log(lake.ReadString(ctx, client.List(ctx, "test_trace")))
	// 	}()
	// }
	// Get trace and print
	tr := trace.FromContext(ctx)
	fmt.Println(tr.Dump())
	time.Sleep(2 * time.Second)

	t.Logf("Total time: %v", tr.Total())

}
func TestWriteWithTrace(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")

	// Create context with trace enabled (operation name auto-detected or specified)
	ctx := trace.WithTrace(context.Background())

	catalog := "test_trace"
	// Write with trace
	err := client.Write(ctx, lake.WriteRequest{
		Catalog:   catalog,
		Path:      "/user.info/profile.data",
		Body:      []byte(`"Bob"`),
		MergeType: lake.MergeTypeReplace,
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	t.Log(lake.ReadString(ctx, client.List(ctx, "test_trace")))
	// Get trace and print
	tr := trace.FromContext(ctx)
	fmt.Println(tr.Dump())

	// t.Logf("Total time: %v", tr.Total())
}

func TestMultipleWritesWithTrace(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
	catalog := "test_multi_trace"

	// Write 3 times with trace
	for i := 0; i < 3; i++ {
		ctx := trace.WithTrace(context.Background(), fmt.Sprintf("Write_%d", i))

		err := client.Write(ctx, lake.WriteRequest{
			Catalog:   catalog,
			Path:      fmt.Sprintf("/field_%d", i),
			Body:      []byte(fmt.Sprintf(`"value_%d"`, i)),
			MergeType: lake.MergeTypeReplace,
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

	err := client.Write(ctx, lake.WriteRequest{
		Catalog:   "test_no_trace",
		Path:      "/data",
		Body:      []byte(`"test"`),
		MergeType: lake.MergeTypeReplace,
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
