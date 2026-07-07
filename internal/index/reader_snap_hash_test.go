package index

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// indexTestRedis returns a real Redis client (db 13 — index data, not cache)
// and a unique key prefix, or skips when Redis is unreachable. Shared by every
// index Redis test (snap hash, notify). It never flushes the DB: cleanup
// deletes only this test's "<prefix>:*" keys, so any other data is untouched.
// The address defaults to 127.0.0.1:6379; LAKE_TEST_REDIS_ADDR overrides it
// (same variable the root package's helper honours).
func indexTestRedis(t *testing.T) (*redis.Client, string) {
	t.Helper()
	addr := os.Getenv("LAKE_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	// MaxRetries -1 / DialerRetries 1: fail the skip probe fast when Redis is
	// absent instead of burning the ping context in go-redis retry loops.
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 13, DialTimeout: 200 * time.Millisecond, MaxRetries: -1, DialerRetries: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable, skipping integration test: %v", err)
	}
	prefix := fmt.Sprintf("laketest_%d_%s", os.Getpid(), strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() {
		c := context.Background()
		var cursor uint64
		for {
			keys, next, err := rdb.Scan(c, cursor, prefix+":*", 256).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				rdb.Del(c, keys...)
			}
			if next == 0 {
				break
			}
			cursor = next
		}
		_ = rdb.Close()
	})
	return rdb, prefix
}

// TestSnapHashRoundTrip exercises the AddSnap → HGet path on the real
// Redis Hash and verifies the layout is "<prefix>:s" with catalog as
// field, value = "{stopTsSeq}".
func TestSnapHashRoundTrip(t *testing.T) {
	rdb, prefix := indexTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix(prefix)
	r.SetPrefix(prefix)

	ctx := context.Background()
	stop := TimeSeqID{Timestamp: 1700000100, SeqID: 500}
	uri := "oss://my-bucket/4f3a/(users/1700000100_500.snap"

	if err := w.AddSnap(ctx, "users", stop, uri, ""); err != nil {
		t.Fatalf("AddSnap: %v", err)
	}

	val, err := rdb.HGet(ctx, r.MakeSnapsHashKey(), "users").Result()
	if err != nil {
		t.Fatalf("HGet: %v", err)
	}
	want, _ := EncodeSnapValue(stop, uri)
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
	if got.URI != uri {
		t.Fatalf("URI: got %q, want %q", got.URI, uri)
	}
	if got.Score() != stop.Score() {
		t.Fatalf("Score(): got %v, want %v", got.Score(), stop.Score())
	}
}

// TestSnapHashOverwrite confirms the V3 contract: an AddSnap with a newer
// stop overwrites the catalog's previous entry (still one field per catalog).
func TestSnapHashOverwrite(t *testing.T) {
	rdb, prefix := indexTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix(prefix)
	r.SetPrefix(prefix)

	ctx := context.Background()
	stop1 := TimeSeqID{Timestamp: 1700000100, SeqID: 500}
	stop2 := TimeSeqID{Timestamp: 1700000200, SeqID: 999}

	if err := w.AddSnap(ctx, "users", stop1, "oss://b/"+stop1.String()+".snap", ""); err != nil {
		t.Fatalf("first AddSnap: %v", err)
	}
	if err := w.AddSnap(ctx, "users", stop2, "oss://b/"+stop2.String()+".snap", ""); err != nil {
		t.Fatalf("second AddSnap: %v", err)
	}

	got, err := r.GetLatestSnap(ctx, "users")
	if err != nil {
		t.Fatalf("GetLatestSnap: %v", err)
	}
	if got.StopTsSeq != stop2 {
		t.Fatalf("after overwrite: got %+v, want stop=%v", got, stop2)
	}

	cnt, err := rdb.HLen(ctx, r.MakeSnapsHashKey()).Result()
	if err != nil {
		t.Fatalf("HLen: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("HLen: got %d, want 1 (one catalog, one field)", cnt)
	}
}

// TestSnapHashMonotonic pins AddSnap's anti-regression guard: snapshot saves
// are async and may race across processes, so a save computed at an OLDER
// stop that lands late must NOT regress the snap pointer. Equal stops keep
// the stored entry too (first writer wins). Run against real Redis because
// the guard lives in Lua.
func TestSnapHashMonotonic(t *testing.T) {
	rdb, prefix := indexTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix(prefix)
	r.SetPrefix(prefix)

	ctx := context.Background()
	older := TimeSeqID{Timestamp: 1700000100, SeqID: 500}
	newer := TimeSeqID{Timestamp: 1700000200, SeqID: 1}

	if err := w.AddSnap(ctx, "users", newer, "oss://b/"+newer.String()+".snap", ""); err != nil {
		t.Fatalf("AddSnap newer: %v", err)
	}
	// A late save at an older stop is silently dropped.
	if err := w.AddSnap(ctx, "users", older, "oss://b/"+older.String()+".snap", ""); err != nil {
		t.Fatalf("AddSnap older: %v", err)
	}
	// Same stop, different uri: stored entry wins.
	if err := w.AddSnap(ctx, "users", newer, "oss://elsewhere/"+newer.String()+".snap", ""); err != nil {
		t.Fatalf("AddSnap equal: %v", err)
	}

	got, err := r.GetLatestSnap(ctx, "users")
	if err != nil {
		t.Fatalf("GetLatestSnap: %v", err)
	}
	if got.StopTsSeq != newer {
		t.Fatalf("snap pointer regressed: got stop=%v, want %v", got.StopTsSeq, newer)
	}
	if got.URI != "oss://b/"+newer.String()+".snap" {
		t.Fatalf("equal-stop AddSnap replaced the entry: uri=%q", got.URI)
	}

	// A stored value the Go reader rejects must not wedge the catalog: AddSnap
	// treats it as absent and overwrites (self-heal) — even at an older stop,
	// and even when the corrupt value carries a huge tsSeq score. The keep
	// branch in Lua must therefore mirror DecodeSnapValue exactly; these are
	// the shapes that score high in a laxer decoder but fail the Go one.
	for _, corrupt := range []string{
		"not-json",
		`["9999999999_1"]`,                 // missing uri
		`["9999999999_1",""]`,              // empty uri
		`["9999999999_1",42]`,              // non-string uri
		`["99999999999999_1","oss://x"]`,   // ts past the year-3000 cap
		`["09999999999_1","oss://x"]`,      // leading-zero ts
		`["9999999999_0","oss://x"]`,       // seq 0 outside 1..999999
		`["9999999999_011","oss://x"]`,     // leading-zero seq
		`["9999999999_1000000","oss://x"]`, // seq past 999999
	} {
		if err := rdb.HSet(ctx, r.MakeSnapsHashKey(), "users", corrupt).Err(); err != nil {
			t.Fatalf("HSet corrupt %q: %v", corrupt, err)
		}
		if err := w.AddSnap(ctx, "users", older, "oss://b/"+older.String()+".snap", ""); err != nil {
			t.Fatalf("AddSnap over corrupt %q: %v", corrupt, err)
		}
		got, err = r.GetLatestSnap(ctx, "users")
		if err != nil {
			t.Fatalf("GetLatestSnap after healing %q: %v", corrupt, err)
		}
		if got.StopTsSeq != older {
			t.Fatalf("corrupt entry %q not healed: got stop=%v, want %v", corrupt, got.StopTsSeq, older)
		}
	}
}

// TestIterateSnapsBatchBackup is the headline use-case test: IterateSnaps
// (via HSCAN under the hood) yields every catalog's snap so backup tooling
// can enumerate the full set of OSS snap keys without an OSS LIST.
func TestIterateSnapsBatchBackup(t *testing.T) {
	rdb, prefix := indexTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix(prefix)
	r.SetPrefix(prefix)

	ctx := context.Background()
	stops := map[string]TimeSeqID{
		"users":    {1700000100, 500},
		"orders":   {1700000110, 999},
		"products": {1700000050, 7},
	}

	for catalog, stop := range stops {
		if err := w.AddSnap(ctx, catalog, stop, "oss://b/"+stop.String()+".snap", ""); err != nil {
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
	rdb, prefix := indexTestRedis(t)
	r := NewReader(rdb)
	r.SetPrefix(prefix)

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
	rdb, prefix := indexTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix(prefix)
	r.SetPrefix(prefix)

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		cat := fmt.Sprintf("c%02d", i)
		ts := TimeSeqID{Timestamp: 1700000000 + int64(i), SeqID: 1}
		if err := w.AddSnap(ctx, cat, ts, "oss://b/"+ts.String()+".snap", ""); err != nil {
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
