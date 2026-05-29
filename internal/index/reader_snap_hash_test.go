package index

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// snapHashTestRedis returns a real Redis client pointed at db 14, or
// skips the test when Redis is unreachable. db 14 is a dedicated test
// space and is FLUSHDB'd at the start of each test.
func snapHashTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:6379",
		DB:          14,
		DialTimeout: 200 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable, skipping integration test: %v", err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB failed: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// TestSnapHashRoundTrip exercises the AddSnap → HGet path on the real
// Redis Hash and verifies the layout is "<prefix>:s" with catalog as
// field, value = "{stopTsSeq}".
func TestSnapHashRoundTrip(t *testing.T) {
	rdb := snapHashTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix("test")
	r.SetPrefix("test")

	ctx := context.Background()
	stop := TimeSeqID{Timestamp: 1700000100, SeqID: 500}

	if err := w.AddSnap(ctx, "users", stop); err != nil {
		t.Fatalf("AddSnap: %v", err)
	}

	val, err := rdb.HGet(ctx, "test:s", "users").Result()
	if err != nil {
		t.Fatalf("HGet: %v", err)
	}
	want := EncodeSnapValue(stop)
	if val != want {
		t.Fatalf("hash value: got %q, want %q", val, want)
	}

	got, err := r.GetLatestSnap(ctx, "users")
	if err != nil {
		t.Fatalf("GetLatestSnap: %v", err)
	}
	if got == nil {
		t.Fatal("GetLatestSnap returned nil")
	}
	if got.StopTsSeq != stop {
		t.Fatalf("got %+v, want stop=%v", got, stop)
	}
	if got.Score() != stop.Score() {
		t.Fatalf("Score(): got %v, want %v", got.Score(), stop.Score())
	}
}

// TestSnapHashOverwrite confirms the V3 contract: each AddSnap on a
// catalog overwrites its previous entry.
func TestSnapHashOverwrite(t *testing.T) {
	rdb := snapHashTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix("test")
	r.SetPrefix("test")

	ctx := context.Background()
	stop1 := TimeSeqID{Timestamp: 1700000100, SeqID: 500}
	stop2 := TimeSeqID{Timestamp: 1700000200, SeqID: 999}

	if err := w.AddSnap(ctx, "users", stop1); err != nil {
		t.Fatalf("first AddSnap: %v", err)
	}
	if err := w.AddSnap(ctx, "users", stop2); err != nil {
		t.Fatalf("second AddSnap: %v", err)
	}

	got, err := r.GetLatestSnap(ctx, "users")
	if err != nil {
		t.Fatalf("GetLatestSnap: %v", err)
	}
	if got.StopTsSeq != stop2 {
		t.Fatalf("after overwrite: got %+v, want stop=%v", got, stop2)
	}

	cnt, err := rdb.HLen(ctx, "test:s").Result()
	if err != nil {
		t.Fatalf("HLen: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("HLen: got %d, want 1 (one catalog, one field)", cnt)
	}
}

// TestIterateSnapsBatchBackup is the headline use-case test: IterateSnaps
// (via HSCAN under the hood) yields every catalog's snap so backup tooling
// can enumerate the full set of OSS snap keys without an OSS LIST.
func TestIterateSnapsBatchBackup(t *testing.T) {
	rdb := snapHashTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix("test")
	r.SetPrefix("test")

	ctx := context.Background()
	stops := map[string]TimeSeqID{
		"users":    {1700000100, 500},
		"orders":   {1700000110, 999},
		"products": {1700000050, 7},
	}

	for catalog, stop := range stops {
		if err := w.AddSnap(ctx, catalog, stop); err != nil {
			t.Fatalf("AddSnap %s: %v", catalog, err)
		}
	}

	all := make(map[string]SnapInfo)
	if err := r.IterateSnaps(ctx, func(catalog string, snap SnapInfo) bool {
		all[catalog] = snap
		return true
	}); err != nil {
		t.Fatalf("IterateSnaps: %v", err)
	}
	if got := len(all); got != len(stops) {
		t.Fatalf("IterateSnaps count: got %d, want %d", got, len(stops))
	}
	for catalog, want := range stops {
		got, ok := all[catalog]
		if !ok {
			t.Errorf("missing catalog %q from IterateSnaps", catalog)
			continue
		}
		if got.StopTsSeq != want {
			t.Errorf("catalog %q: got %+v, want stop=%v", catalog, got, want)
		}
	}
}

// TestGetLatestSnapMissingReturnsNilNil covers the "no snap yet" path.
func TestGetLatestSnapMissingReturnsNilNil(t *testing.T) {
	rdb := snapHashTestRedis(t)
	r := NewReader(rdb)
	r.SetPrefix("test")

	ctx := context.Background()
	got, err := r.GetLatestSnap(ctx, "never-written")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil SnapInfo, got %+v", got)
	}
}

// TestIterateSnapsEarlyStop confirms IterateSnaps honours fn returning
// false (caller stops iteration mid-stream) without consuming the whole
// hash — the property backup tools rely on for budgeted scans.
func TestIterateSnapsEarlyStop(t *testing.T) {
	rdb := snapHashTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix("test")
	r.SetPrefix("test")

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		cat := fmt.Sprintf("c%02d", i)
		if err := w.AddSnap(ctx, cat, TimeSeqID{Timestamp: 1700000000 + int64(i), SeqID: 1}); err != nil {
			t.Fatalf("AddSnap %s: %v", cat, err)
		}
	}

	var seen int
	err := r.IterateSnaps(ctx, func(string, SnapInfo) bool {
		seen++
		return seen < 3 // request stop after the 3rd item
	})
	if err != nil {
		t.Fatalf("IterateSnaps: %v", err)
	}
	if seen != 3 {
		t.Fatalf("early-stop: callback ran %d times, want 3", seen)
	}
}
