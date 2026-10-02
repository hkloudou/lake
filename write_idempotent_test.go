package lake

import (
	"context"
	"testing"

	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
	"github.com/tidwall/gjson"
)

// TestWriteNotify_IdempotentRetry_Redis pins the retry contract: a handle
// whose Notify succeeded but whose response was lost is retried by the
// client, possibly after other writes landed. The retry must return success
// WITHOUT appending a second delta — otherwise the retried body gets a newer
// tsSeq than the writes in between and silently overwrites them.
func TestWriteNotify_IdempotentRetry_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, _, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve)
	ctx := context.Background()

	begin := func(body string) *WriteHandle {
		t.Helper()
		h, err := beginWrite(c, WriteRequest{
			Catalog: "acct", Path: "/x", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
		})
		if err != nil {
			t.Fatalf("NewWriteHandle: %v", err)
		}
		if err := upload(store, h, body); err != nil {
			t.Fatalf("upload: %v", err)
		}
		return h
	}

	hA := begin(`1`)
	if err := c.WriteNotify(ctx, hA); err != nil {
		t.Fatalf("Notify A: %v", err)
	}
	hB := begin(`2`)
	if err := c.WriteNotify(ctx, hB); err != nil {
		t.Fatalf("Notify B: %v", err)
	}
	// The client never saw A's response and retries it after B landed.
	if err := c.WriteNotify(ctx, hA); err != nil {
		t.Fatalf("retried Notify A must succeed: %v", err)
	}

	list := c.List(ctx, "acct")
	if list.Err != nil {
		t.Fatal(list.Err)
	}
	if len(list.Entries) != 2 {
		t.Fatalf("entries = %d, want 2 (the retry must not append)", len(list.Entries))
	}
	got, err := ReadString(ctx, list)
	if err != nil {
		t.Fatal(err)
	}
	if gjson.Get(got, "x").Int() != 2 {
		t.Fatalf("doc = %s, want /x=2 (B must not be overwritten by A's retry)", got)
	}

	// A retry of a write the operator removed must not resurrect it.
	if removed, err := c.RemoveDelta(ctx, "acct", list.Entries[0].TsSeq.String()); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}
	if err := c.WriteNotify(ctx, hA); err != nil {
		t.Fatalf("Notify A after removal: %v", err)
	}
	if n := len(c.List(ctx, "acct").Entries); n != 1 {
		t.Fatalf("entries after removal + retry = %d, want 1", n)
	}
}
