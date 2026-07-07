package index

import (
	"context"
	"testing"
)

// TestScriptDispatchSurvivesScriptFlush pins the EVALSHA dispatch against a
// cold server script cache — the state after a Redis restart or a SCRIPT
// FLUSH. Single calls recover via Script.Run's built-in EVAL fallback;
// BatchList cannot (a pipelined command's error surfaces only at Exec), so it
// carries its own load-and-retry, and this test exercises both paths.
// SCRIPT FLUSH clears only the server's script cache, never data — any
// EVALSHA client must tolerate it, so flushing on a shared dev Redis is safe.
func TestScriptDispatchSurvivesScriptFlush(t *testing.T) {
	rdb, prefix := indexTestRedis(t)
	w := NewWriter(rdb)
	r := NewReader(rdb)
	w.SetPrefix(prefix)
	r.SetPrefix(prefix)
	ctx := context.Background()

	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	tsSeq, _, err := w.Notify(ctx, "users", "/profile", MergeTypeReplace, "oss://b/x.dat")
	if err != nil {
		t.Fatalf("Notify on cold script cache: %v", err)
	}

	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	snap, rr := r.ListCatalog(ctx, "users")
	if rr.Err != nil {
		t.Fatalf("ListCatalog on cold script cache: %v", rr.Err)
	}
	if snap != nil || len(rr.Deltas) != 1 || rr.Deltas[0].TsSeq != tsSeq {
		t.Fatalf("ListCatalog: got snap=%v deltas=%+v, want the one notified delta", snap, rr.Deltas)
	}

	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	out := r.BatchList(ctx, []string{"users", "empty-cat"})
	for cat, br := range out {
		if br.ReadResult == nil {
			t.Fatalf("BatchList[%s]: nil ReadResult", cat)
		}
		if br.ReadResult.Err != nil {
			t.Fatalf("BatchList[%s] on cold script cache: %v", cat, br.ReadResult.Err)
		}
	}
	if got := len(out["users"].ReadResult.Deltas); got != 1 {
		t.Fatalf("BatchList[users]: got %d deltas, want 1", got)
	}
	if got := len(out["empty-cat"].ReadResult.Deltas); got != 0 {
		t.Fatalf("BatchList[empty-cat]: got %d deltas, want 0", got)
	}
}
