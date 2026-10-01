package index

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestNotifyMemberConsistency_Redis pins the Lua↔Go contract: the notify
// script is the sole encoder of a delta (member via cjson, score via
// ts + seq/1e6) and DecodeDeltaMember + TimeSeqID.Score are the decoders.
// Only a real Redis catches a drift between the two. It also pins notify's
// idempotency: a repeat call for the same uri returns the original tsSeq and
// appends nothing.
func TestNotifyMemberConsistency_Redis(t *testing.T) {
	rdb, prefix := indexTestRedis(t)
	x := New(rdb, prefix)
	ctx := context.Background()
	const catalog = "users"

	ts1, err := x.Notify(ctx, catalog, "/profile", MergeTypeRFC7396, "oss://bucket/4f3a/(users/a.dat")
	if err != nil {
		t.Fatalf("Notify #1: %v", err)
	}
	if _, err := x.Notify(ctx, catalog, "/", MergeTypeReplace, "oss://bucket/4f3a/(users/b.dat"); err != nil {
		t.Fatalf("Notify #2: %v", err)
	}
	again, err := x.Notify(ctx, catalog, "/profile", MergeTypeRFC7396, "oss://bucket/4f3a/(users/a.dat")
	if err != nil || again != ts1 {
		t.Fatalf("repeat Notify = %v/%v, want the original %v (idempotent)", again, err, ts1)
	}

	zs, err := rdb.ZRangeByScoreWithScores(ctx, x.deltaKey(catalog), &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
	if err != nil {
		t.Fatalf("zrange: %v", err)
	}
	if len(zs) != 2 {
		t.Fatalf("zset entries = %d, want 2 (the repeat must not append)", len(zs))
	}
	for _, z := range zs {
		d, derr := DecodeDeltaMember(z.Member.(string), z.Score)
		if derr != nil {
			t.Fatalf("DecodeDeltaMember(%q, %.6f): %v — Lua member drifted from the Go decoder", z.Member, z.Score, derr)
		}
		if d.TsSeq.Score() != z.Score {
			t.Fatalf("score lockstep broken for %s: Go=%v, Redis=%v", d.TsSeq, d.TsSeq.Score(), z.Score)
		}
	}
	if zs[0].Member.(string) == zs[1].Member.(string) || DecodeOrFatal(t, zs[0]).TsSeq != ts1 {
		t.Fatalf("first stored member %q is not the one Notify #1 returned (%v)", zs[0].Member, ts1)
	}
}

func DecodeOrFatal(t *testing.T, z redis.Z) *DeltaInfo {
	t.Helper()
	d, err := DecodeDeltaMember(z.Member.(string), z.Score)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
