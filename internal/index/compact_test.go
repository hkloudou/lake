package index

import (
	"context"
	"fmt"
	"testing"
)

// TestCompactDeltas drives the real notify Lua for deltas, then pins the
// compaction contract against live Redis: nothing is trimmed without a
// decodable snap pointer; the trim bound is inclusive of the stop; the call
// is idempotent; entries past the stop survive.
func TestCompactDeltas(t *testing.T) {
	rdb, prefix := indexTestRedis(t)
	x := New(rdb, prefix)

	ctx := context.Background()
	const catalog = "users"
	const uri = "oss://bucket/4f3a/(users/abc.dat"

	// No snap yet → no-op.
	n, err := x.Compact(ctx, catalog)
	if err != nil {
		t.Fatalf("CompactDeltas (no snap): %v", err)
	}
	if n != 0 {
		t.Fatalf("no-snap compact removed %d entries, want 0", n)
	}

	var stops []TimeSeqID
	for i := 0; i < 3; i++ {
		ts, err := x.Notify(ctx, catalog, "/", MergeTypeReplace, fmt.Sprintf("%s%d", uri, i))
		if err != nil {
			t.Fatalf("Notify #%d: %v", i, err)
		}
		stops = append(stops, ts)
	}

	// A snap pointer the Go reader rejects must never authorize a trim, no
	// matter how high it scores.
	if err := rdb.HSet(ctx, x.snapsKey(), catalog, `["9999999999_1",""]`).Err(); err != nil {
		t.Fatalf("HSet corrupt snap: %v", err)
	}
	if n, err = x.Compact(ctx, catalog); err != nil || n != 0 {
		t.Fatalf("corrupt-snap compact: removed=%d err=%v, want 0/nil", n, err)
	}
	if card := rdb.ZCard(ctx, x.deltaKey(catalog)).Val(); card != 3 {
		t.Fatalf("zset after corrupt-snap compact: %d entries, want 3", card)
	}

	// Snap at the 2nd delta absorbs the first two (bound is inclusive).
	// AddSnap self-heals over the corrupt value planted above.
	if err := x.AddSnap(ctx, catalog, stops[1], "oss://b/"+stops[1].String()+".snap", ""); err != nil {
		t.Fatalf("AddSnap: %v", err)
	}
	if n, err = x.Compact(ctx, catalog); err != nil || n != 2 {
		t.Fatalf("compact: removed=%d err=%v, want 2/nil", n, err)
	}

	// The read primitive sees the same world after compaction: snap at
	// stops[1], exactly the one delta past it.
	rr := x.List(ctx, catalog)
	snap := rr.Snap
	if rr.Err != nil {
		t.Fatalf("List after compact: %v", rr.Err)
	}
	if snap == nil || snap.StopTsSeq != stops[1] {
		t.Fatalf("snap after compact: %+v, want stop=%v", snap, stops[1])
	}
	if len(rr.Deltas) != 1 || rr.Deltas[0].TsSeq != stops[2] {
		t.Fatalf("deltas after compact: %+v, want single entry at %v", rr.Deltas, stops[2])
	}

	// Idempotent: nothing left at or below the stop.
	if n, err = x.Compact(ctx, catalog); err != nil || n != 0 {
		t.Fatalf("second compact: removed=%d err=%v, want 0/nil", n, err)
	}
}
