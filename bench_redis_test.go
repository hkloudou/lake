package lake

import (
	"context"
	"fmt"
	"testing"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/merge"
	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
)

// Benchmarks talk to the same local Redis as the integration tests
// (127.0.0.1:6379, via the shared testing.TB helpers) and skip when it is
// unreachable. Each run uses a unique prefix and deletes only its own keys.

// benchClient wires a Client against local Redis + in-memory storage and
// seeds the catalog with nDeltas root-replace writes.
func benchClient(b *testing.B, nDeltas int) (*Client, context.Context) {
	rdb := redisTestDB(b, 13)
	prefix := testPrefix(b)
	cleanupKeys(b, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, _, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve)

	ctx := context.Background()
	for i := 0; i < nDeltas; i++ {
		h, err := c.WriteBegin(ctx, WriteBeginRequest{
			Catalog: "bench", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
		})
		if err != nil {
			b.Fatalf("WriteBegin: %v", err)
		}
		body := fmt.Sprintf(`{"n":%d,"payload":"0123456789abcdef"}`, i)
		if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(body)); err != nil {
			b.Fatalf("upload: %v", err)
		}
		if err := c.WriteNotify(ctx, h); err != nil {
			b.Fatalf("WriteNotify: %v", err)
		}
	}
	return c, ctx
}

// BenchmarkList measures the atomic list script round-trip: one snapless
// catalog with 3 deltas — the everyday read-index cost.
func BenchmarkList(b *testing.B) {
	c, ctx := benchClient(b, 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if lr := c.List(ctx, "bench"); lr.Err != nil {
			b.Fatal(lr.Err)
		}
	}
}

// BenchmarkBatchList100 measures 100 catalogs listed in one pipeline (99 of
// them empty — the shape of a fleet dashboard poll).
func BenchmarkBatchList100(b *testing.B) {
	c, ctx := benchClient(b, 3)
	catalogs := make([]string, 100)
	for i := range catalogs {
		catalogs[i] = fmt.Sprintf("bench/other-%d", i)
	}
	catalogs[0] = "bench"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := c.BatchList(ctx, catalogs)
		if r := out["bench"]; r == nil || r.Err != nil {
			b.Fatalf("BatchList: %v", r)
		}
	}
}

// BenchmarkNotify measures the write-commit script (seqid alloc + ZADD).
func BenchmarkNotify(b *testing.B) {
	c, ctx := benchClient(b, 0)
	h, err := c.WriteBegin(ctx, WriteBeginRequest{
		Catalog: "bench", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
	})
	if err != nil {
		b.Fatalf("WriteBegin: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := c.writer.Notify(ctx, "bench", "/", MergeTypeReplace, h.URI); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRead measures the full read path (list → parallel body fetch from
// mem storage → merge) for a 10-delta catalog without snapshotting.
func BenchmarkRead(b *testing.B) {
	c, ctx := benchClient(b, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lr := c.List(ctx, "bench")
		if lr.Err != nil {
			b.Fatal(lr.Err)
		}
		if _, err := ReadBytes(ctx, lr); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSampleHit measures the memoised sample fast path (pipelined probe,
// cache hit, no loader run).
func BenchmarkSampleHit(b *testing.B) {
	c, ctx := benchClient(b, 3)
	sampler := NewSampler[int]("bench-ind", func(lr *ListResult) (int, error) {
		return len(lr.Entries), nil
	})
	lr := c.List(ctx, "bench")
	if lr.Err != nil {
		b.Fatal(lr.Err)
	}
	if _, err := sampler.Sample(ctx, lr); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sampler.Sample(ctx, lr); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMerge50 measures pure merge CPU for a 50-delta RFC7396 chain on a
// modest document — no Redis, no storage.
func BenchmarkMerge50(b *testing.B) {
	base := []byte(`{"a":{"b":1,"c":[1,2,3]},"d":"x"}`)
	entries := make([]index.DeltaInfo, 0, 50)
	for i := 0; i < 50; i++ {
		entries = append(entries, index.DeltaInfo{
			MergeType: index.MergeTypeRFC7396,
			Path:      "/",
			Body:      []byte(fmt.Sprintf(`{"k%d":%d}`, i%7, i)),
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := merge.Merge(base, entries); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMerge50Scoped measures a 50-delta chain of FIELD-scoped writes on
// a larger document — the shape where the owned-buffer in-place path pays.
func BenchmarkMerge50Scoped(b *testing.B) {
	var doc []byte
	doc = append(doc, '{')
	for i := 0; i < 200; i++ {
		if i > 0 {
			doc = append(doc, ',')
		}
		doc = append(doc, fmt.Sprintf(`"f%d":{"v":%d,"s":"0123456789abcdef"}`, i, i)...)
	}
	doc = append(doc, '}')

	entries := make([]index.DeltaInfo, 0, 50)
	for i := 0; i < 50; i++ {
		entries = append(entries, index.DeltaInfo{
			MergeType: index.MergeTypeReplace,
			Path:      fmt.Sprintf("/f%d/v", i*3),
			Body:      []byte(fmt.Sprintf("%d", i+1000)),
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := merge.Merge(doc, entries); err != nil {
			b.Fatal(err)
		}
	}
}
