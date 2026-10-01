package index

import "testing"

// TestBatchListSurvivesScriptFlush: a pipelined EVALSHA cannot fall back to
// EVAL on its own (the error surfaces only at Exec), so BatchList carries its
// own retry — exercised here against a freshly flushed script cache.
func TestBatchListSurvivesScriptFlush(t *testing.T) {
	x, ctx := scriptFlushRedis(t)
	if _, err := x.Notify(ctx, "users", "/", MergeTypeReplace, "oss://b/x.dat"); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if err := x.rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	out := x.BatchList(ctx, []string{"users", "empty-cat"})
	for cat, l := range out {
		if l.Err != nil {
			t.Fatalf("BatchList[%s] on cold script cache: %v", cat, l.Err)
		}
	}
	if got := len(out["users"].Deltas); got != 1 {
		t.Fatalf("BatchList[users]: got %d deltas, want 1", got)
	}
	if got := len(out["empty-cat"].Deltas); got != 0 {
		t.Fatalf("BatchList[empty-cat]: got %d deltas, want 0", got)
	}
}
