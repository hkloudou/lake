package index

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestNotifyMemberConsistency_Redis pins the Lua↔Go contract that nothing else
// fully exercises. The notify script (writer_atomic.go) is the *sole* encoder of
// a delta: it builds the member [mergeType, path, tsSeq, uri] via cjson and the
// score via `ts + seqid/1e6`, all server-side. DecodeDeltaMember + TimeSeqID.Score
// are the Go *decoders*. The two live in different languages and must agree
// byte-for-byte (member shape, number formatting) and bit-for-bit (score). The
// unit tests build members with Go's json.Marshal, so only a real Redis catches a
// drift. Skips when Redis is unreachable.
func TestNotifyMemberConsistency_Redis(t *testing.T) {
	rdb, prefix := indexTestRedis(t)
	w := NewWriter(rdb)
	w.SetPrefix(prefix)
	ctx := context.Background()

	const catalog = "users"
	const uri = "oss://bucket/4f3a/(users/abc.dat"

	// Drive the real notify Lua twice (distinct merge types / paths).
	ts1, member1, err := w.Notify(ctx, catalog, "/profile", MergeTypeRFC7396, uri)
	if err != nil {
		t.Fatalf("Notify #1: %v", err)
	}
	if _, _, err := w.Notify(ctx, catalog, "/", MergeTypeReplace, uri); err != nil {
		t.Fatalf("Notify #2: %v", err)
	}
	if ts1.SeqID < 1 {
		t.Fatalf("seqid must be >= 1, got %d", ts1.SeqID)
	}

	// Read the zset back exactly as the read path does.
	zs, err := rdb.ZRangeByScoreWithScores(ctx, w.MakeDeltaZsetKey(catalog),
		&redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Result()
	if err != nil {
		t.Fatalf("zrange: %v", err)
	}
	if len(zs) != 2 {
		t.Fatalf("zset entries = %d, want 2", len(zs))
	}

	var sawMember1 bool
	for _, z := range zs {
		member := z.Member.(string)
		if member == member1 {
			sawMember1 = true
		}
		// (1) The cjson-encoded member must decode through the Go reader — this
		// is what catches a Lua/cjson shape or number-format drift that the
		// json.Marshal-based unit tests cannot see.
		d, derr := DecodeDeltaMember(member, z.Score)
		if derr != nil {
			t.Fatalf("DecodeDeltaMember(%q, %.6f): %v — Lua member drifted from the Go decoder", member, z.Score, derr)
		}
		// (2) Explicit score lockstep: the score Go recomputes from the decoded
		// tsSeq must bit-match the score Lua computed and Redis stored. (Decode
		// already enforces this; re-assert so weakening that guard fails here.)
		if d.TsSeq.Score() != z.Score {
			t.Fatalf("score lockstep broken for %s: Go=%v, Redis=%v", d.TsSeq, d.TsSeq.Score(), z.Score)
		}
	}
	if !sawMember1 {
		t.Fatalf("Notify returned member %q but it is not the one stored in the zset", member1)
	}
}
