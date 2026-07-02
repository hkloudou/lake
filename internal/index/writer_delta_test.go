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

	// Neighbours survive, in order.
	_, rr := r.ListCatalog(ctx, catalog)
	if rr.Err != nil {
		t.Fatalf("ListCatalog: %v", rr.Err)
	}
	if len(rr.Deltas) != 2 || rr.Deltas[0].TsSeq != stops[0] || rr.Deltas[1].TsSeq != stops[2] {
		t.Fatalf("survivors = %+v, want [%v %v]", rr.Deltas, stops[0], stops[2])
	}
}
