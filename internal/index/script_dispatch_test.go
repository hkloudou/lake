package index

import (
	"context"
	"os"
	"testing"
)

// scriptFlushRedis is indexTestRedis plus a server-wide SCRIPT FLUSH — the
// state after a Redis restart. Opt-in (LAKE_TEST_SCRIPT_FLUSH=1, set by CI
// against its dedicated Redis): the flush clears the script cache for every
// client of the server, which would break an unrelated client that pipelines
// EVALSHA without a reload path, so a shared Redis skips these tests.
func scriptFlushRedis(t *testing.T) (*Index, context.Context) {
	t.Helper()
	if os.Getenv("LAKE_TEST_SCRIPT_FLUSH") != "1" {
		t.Skip("set LAKE_TEST_SCRIPT_FLUSH=1 to run (issues a server-wide SCRIPT FLUSH; only safe on a dedicated Redis)")
	}
	rdb, prefix := indexTestRedis(t)
	ctx := context.Background()
	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	return New(rdb, prefix), ctx
}

// TestScriptDispatchSurvivesScriptFlush: single-call scripts recover from a
// cold script cache through Script.Run's built-in EVAL fallback.
func TestScriptDispatchSurvivesScriptFlush(t *testing.T) {
	x, ctx := scriptFlushRedis(t)
	tsSeq, err := x.Notify(ctx, "users", "/profile", MergeTypeReplace, "oss://b/x.dat")
	if err != nil {
		t.Fatalf("Notify on cold script cache: %v", err)
	}
	if err := x.rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	l := x.List(ctx, "users")
	if l.Err != nil {
		t.Fatalf("List on cold script cache: %v", l.Err)
	}
	if l.Snap != nil || len(l.Deltas) != 1 || l.Deltas[0].TsSeq != tsSeq {
		t.Fatalf("List: got snap=%v deltas=%+v, want the one notified delta", l.Snap, l.Deltas)
	}
}
