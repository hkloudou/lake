package index

import (
	"context"
	"os"
	"testing"
)

// TestScriptDispatchSurvivesScriptFlush pins the EVALSHA dispatch against a
// cold server script cache — the state after a Redis restart or a SCRIPT
// FLUSH. Single calls recover via Script.Run's built-in EVAL fallback;
// BatchList cannot (a pipelined command's error surfaces only at Exec), so it
// carries its own load-and-retry, and this test exercises both paths.
//
// Opt-in (LAKE_TEST_SCRIPT_FLUSH=1, set by CI against its dedicated Redis):
// SCRIPT FLUSH never touches data, but it clears the SERVER-WIDE script cache
// — all logical DBs, all clients — and an unrelated client that pipelines
// EVALSHA without a reload path would see hard NOSCRIPT errors. That would
// break the "safe to point at a Redis that holds other data" promise the
// rest of the suite keeps, so a shared Redis skips this test by default.
func TestScriptDispatchSurvivesScriptFlush(t *testing.T) {
	if os.Getenv("LAKE_TEST_SCRIPT_FLUSH") != "1" {
		t.Skip("set LAKE_TEST_SCRIPT_FLUSH=1 to run (issues a server-wide SCRIPT FLUSH; only safe on a dedicated Redis)")
	}
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
