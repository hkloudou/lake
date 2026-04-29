package index

import (
	"context"
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

// TestSnapHashRoundTrip exercises the AddSnap → GetLatestSnap path on
// the real Redis Hash and verifies the layout is "<prefix>:snaps" with
// catalog as field, value = "{start}|{stop}".
func TestSnapHashRoundTrip(t *testing.T) {
	rdb := snapHashTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	t.Cleanup(func() { r.Close() })
	w.SetPrefix("test")
	r.SetPrefix("test")

	ctx := context.Background()
	start := TimeSeqID{Timestamp: 1700000000, SeqID: 1}
	stop := TimeSeqID{Timestamp: 1700000100, SeqID: 500}

	if err := w.AddSnap(ctx, "users", start, stop); err != nil {
		t.Fatalf("AddSnap: %v", err)
	}

	// Hash key shape and field layout
	val, err := rdb.HGet(ctx, "test:snaps", "users").Result()
	if err != nil {
		t.Fatalf("HGet: %v", err)
	}
	want := EncodeSnapValue(start, stop)
	if val != want {
		t.Fatalf("hash value: got %q, want %q", val, want)
	}

	// GetLatestSnap roundtrips through DecodeSnapValue
	got, err := r.GetLatestSnap(ctx, "users")
	if err != nil {
		t.Fatalf("GetLatestSnap: %v", err)
	}
	if got == nil {
		t.Fatal("GetLatestSnap returned nil")
	}
	if got.StartTsSeq != start || got.StopTsSeq != stop {
		t.Fatalf("got %+v, want start=%v stop=%v", got, start, stop)
	}
	if got.Score() != stop.Score() {
		t.Fatalf("Score(): got %v, want %v", got.Score(), stop.Score())
	}
}

// TestSnapHashOverwrite confirms the V3 contract: each AddSnap on a
// catalog overwrites its previous entry; we never accumulate historical
// snaps in Redis.
func TestSnapHashOverwrite(t *testing.T) {
	rdb := snapHashTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	t.Cleanup(func() { r.Close() })
	w.SetPrefix("test")
	r.SetPrefix("test")

	ctx := context.Background()
	first := TimeSeqID{Timestamp: 1700000000, SeqID: 1}
	stop1 := TimeSeqID{Timestamp: 1700000100, SeqID: 500}
	stop2 := TimeSeqID{Timestamp: 1700000200, SeqID: 999}

	if err := w.AddSnap(ctx, "users", first, stop1); err != nil {
		t.Fatalf("first AddSnap: %v", err)
	}
	if err := w.AddSnap(ctx, "users", stop1, stop2); err != nil {
		t.Fatalf("second AddSnap: %v", err)
	}

	got, err := r.GetLatestSnap(ctx, "users")
	if err != nil {
		t.Fatalf("GetLatestSnap: %v", err)
	}
	if got.StartTsSeq != stop1 || got.StopTsSeq != stop2 {
		t.Fatalf("after overwrite: got %+v, want start=%v stop=%v", got, stop1, stop2)
	}

	// Hash holds exactly one field per catalog.
	cnt, err := rdb.HLen(ctx, "test:snaps").Result()
	if err != nil {
		t.Fatalf("HLen: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("HLen: got %d, want 1 (one catalog, one field)", cnt)
	}
}

// TestAllSnapsBatchBackup is the headline use-case test: HGETALL returns
// every catalog's snap in one call so backup tooling can enumerate the
// full set of OSS snap keys without an OSS LIST.
func TestAllSnapsBatchBackup(t *testing.T) {
	rdb := snapHashTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	t.Cleanup(func() { r.Close() })
	w.SetPrefix("test")
	r.SetPrefix("test")

	ctx := context.Background()
	pairs := map[string]struct {
		start, stop TimeSeqID
	}{
		"users":    {TimeSeqID{1700000000, 1}, TimeSeqID{1700000100, 500}},
		"orders":   {TimeSeqID{1700000010, 2}, TimeSeqID{1700000110, 999}},
		"products": {TimeSeqID{0, 0}, TimeSeqID{1700000050, 7}}, // first snap shape
	}

	for catalog, p := range pairs {
		if err := w.AddSnap(ctx, catalog, p.start, p.stop); err != nil {
			t.Fatalf("AddSnap %s: %v", catalog, err)
		}
	}

	all, err := r.AllSnaps(ctx)
	if err != nil {
		t.Fatalf("AllSnaps: %v", err)
	}
	if got := len(all); got != len(pairs) {
		t.Fatalf("AllSnaps length: got %d, want %d", got, len(pairs))
	}
	for catalog, want := range pairs {
		got, ok := all[catalog]
		if !ok {
			t.Errorf("missing catalog %q in AllSnaps", catalog)
			continue
		}
		if got.StartTsSeq != want.start || got.StopTsSeq != want.stop {
			t.Errorf("catalog %q: got %+v, want start=%v stop=%v", catalog, got, want.start, want.stop)
		}
	}
}

// TestGetLatestSnapMissingReturnsNilNil covers the "no snap yet" path.
func TestGetLatestSnapMissingReturnsNilNil(t *testing.T) {
	rdb := snapHashTestRedis(t)
	r := NewReader(rdb)
	t.Cleanup(func() { r.Close() })
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
