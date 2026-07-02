package index

import (
	"context"
	"testing"
)

// TestRemoveDelta pins the surgical-removal contract against live Redis:
// exactly the entry with the given tsSeq goes away, neighbours at other
// scores survive, and a second call (or a bogus tsSeq) removes nothing.
func TestRemoveDelta(t *testing.T) {
	rdb, prefix := indexTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix(prefix)
	r.SetPrefix(prefix)

	ctx := context.Background()
	const catalog = "users"
	const uri = "oss://bucket/4f3a/(users/abc.dat"

	var stops []TimeSeqID
	for i := 0; i < 3; i++ {
		ts, _, err := w.Notify(ctx, catalog, "/", MergeTypeReplace, uri)
		if err != nil {
			t.Fatalf("Notify #%d: %v", i, err)
		}
		stops = append(stops, ts)
	}

	// Nonexistent tsSeq → false, nothing touched.
	removed, err := w.RemoveDelta(ctx, catalog, TimeSeqID{Timestamp: stops[0].Timestamp + 1000, SeqID: 1})
	if err != nil || removed {
		t.Fatalf("bogus tsSeq: removed=%v err=%v, want false/nil", removed, err)
	}

	// Remove the middle entry.
	removed, err = w.RemoveDelta(ctx, catalog, stops[1])
	if err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v, want true/nil", removed, err)
	}
	// Second call: already gone.
	removed, err = w.RemoveDelta(ctx, catalog, stops[1])
	if err != nil || removed {
		t.Fatalf("second RemoveDelta: removed=%v err=%v, want false/nil", removed, err)
	}

	// Neighbours survive, in order — and the removal generation moved from
	// "0" to "1" (one successful removal; the failed attempts must not bump).
	_, rr := r.ListCatalog(ctx, catalog)
	if rr.Err != nil {
		t.Fatalf("ListCatalog: %v", rr.Err)
	}
	if len(rr.Deltas) != 2 || rr.Deltas[0].TsSeq != stops[0] || rr.Deltas[1].TsSeq != stops[2] {
		t.Fatalf("survivors = %+v, want [%v %v]", rr.Deltas, stops[0], stops[2])
	}
	if rr.RemoveGen != "1" {
		t.Fatalf("RemoveGen = %q, want \"1\" (exactly one successful removal)", rr.RemoveGen)
	}

	// Bump-before-delete: if the generation cannot advance (hand-corrupted
	// non-integer ":rg"), the removal must fail with the delta INTACT — the
	// reverse order would leave it gone under an unmoved generation, and an
	// in-flight old-generation snapshot could resurrect it.
	if err := rdb.HSet(ctx, w.MakeSnapsHashKey(), catalog+":rg", "corrupt").Err(); err != nil {
		t.Fatalf("HSet corrupt rg: %v", err)
	}
	if _, err := w.RemoveDelta(ctx, catalog, stops[0]); err == nil {
		t.Fatal("RemoveDelta with corrupt rg must error")
	}
	if card := rdb.ZCard(ctx, w.MakeDeltaZsetKey(catalog)).Val(); card != 2 {
		t.Fatalf("delta removed despite failed generation bump: %d entries, want 2", card)
	}
	// Healing the field lets the removal proceed.
	if err := rdb.HSet(ctx, w.MakeSnapsHashKey(), catalog+":rg", "1").Err(); err != nil {
		t.Fatalf("heal rg: %v", err)
	}
	if removed, err := w.RemoveDelta(ctx, catalog, stops[0]); err != nil || !removed {
		t.Fatalf("RemoveDelta after heal: removed=%v err=%v", removed, err)
	}
}

// TestAddSnapRemoveGenBarrier pins the snapshot-resurrection guard: a
// snapshot computed from a read that predates a RemoveDelta carries the old
// generation and must be dropped; a snapshot from a post-removal read (new
// generation) lands normally.
func TestAddSnapRemoveGenBarrier(t *testing.T) {
	rdb, prefix := indexTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix(prefix)
	r.SetPrefix(prefix)

	ctx := context.Background()
	const catalog = "users"
	ts, _, err := w.Notify(ctx, catalog, "/", MergeTypeReplace, "oss://b/x.dat")
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}

	// A reader lists the catalog (gen "0"), then the delta is removed.
	_, rr := r.ListCatalog(ctx, catalog)
	if rr.Err != nil || rr.RemoveGen != "0" {
		t.Fatalf("pre-removal list: gen=%q err=%v, want \"0\"/nil", rr.RemoveGen, rr.Err)
	}
	if removed, err := w.RemoveDelta(ctx, catalog, ts); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}

	// The reader's snapshot (baking in the removed delta) must be refused.
	if err := w.AddSnap(ctx, catalog, ts, "oss://b/stale.snap", rr.RemoveGen); err != nil {
		t.Fatalf("AddSnap stale gen: %v", err)
	}
	if got, _ := r.GetLatestSnap(ctx, catalog); got != nil {
		t.Fatalf("stale-generation snapshot landed: %+v — removed delta resurrected", got)
	}

	// A snapshot from a post-removal read (current gen) lands normally.
	_, rr = r.ListCatalog(ctx, catalog)
	if rr.Err != nil || rr.RemoveGen != "1" {
		t.Fatalf("post-removal list: gen=%q err=%v, want \"1\"/nil", rr.RemoveGen, rr.Err)
	}
	if err := w.AddSnap(ctx, catalog, ts, "oss://b/fresh.snap", rr.RemoveGen); err != nil {
		t.Fatalf("AddSnap fresh gen: %v", err)
	}
	got, err := r.GetLatestSnap(ctx, catalog)
	if err != nil || got == nil || got.URI != "oss://b/fresh.snap" {
		t.Fatalf("fresh-generation snapshot missing: got=%+v err=%v", got, err)
	}
}
