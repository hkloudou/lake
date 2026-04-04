package lake_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hkloudou/lake/v2"
)

func TestReadSimple(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
	ctx := context.Background()
	t.Log(lake.ReadString(ctx, client.List(ctx, "test_trace")))
	time.Sleep(2 * time.Second)
}

func TestWriteWithTrace(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
	ctx := context.Background()

	catalog := "test_trace"
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
}

func TestMultipleWritesWithTrace(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
	catalog := "test_multi_trace"

	for i := 0; i < 3; i++ {
		ctx := context.Background()

		err := client.Write(ctx, lake.WriteRequest{
			Catalog:   catalog,
			Path:      fmt.Sprintf("/field_%d", i),
			Body:      []byte(fmt.Sprintf(`"value_%d"`, i)),
			MergeType: lake.MergeTypeReplace,
		})
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}
}

func TestWriteWithoutTrace(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
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

	t.Log("✓ Write without trace successful (no logging)")
}
