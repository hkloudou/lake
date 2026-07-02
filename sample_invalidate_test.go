package lake

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hkloudou/lake/v3/internal/index"
)

func TestInvalidateSamples_ValidatesBeforeRedis(t *testing.T) {
	c := newDeadClient(t)
	if _, err := c.InvalidateSamples(context.Background(), "bad|indicator", "users"); err == nil || !strings.Contains(err.Error(), "invalid indicator") {
		t.Fatalf("expected invalid indicator error, got %v", err)
	}
	if _, err := c.InvalidateSamples(context.Background(), "views", "bad|name"); err == nil {
		t.Fatal("expected invalid catalog error, got nil")
	}
	// No catalogs → trivially nothing to do, no Redis call.
	if n, err := c.InvalidateSamples(context.Background(), "views"); err != nil || n != 0 {
		t.Fatalf("empty catalogs: n=%d err=%v, want 0/nil", n, err)
	}
}

// TestInvalidateSamples_ForcesRecompute_Redis pins the full memo lifecycle:
// compute → cached (loader not re-run) → InvalidateSamples → recomputed.
// This is the escape hatch for the two cases staleness policies cannot see —
// a deleted catalog's lingering memo field, and a loader whose code changed
// under an unchanged data version.
func TestInvalidateSamples_ForcesRecompute_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")
	c := New(prefix, rdb, memResolver())

	ctx := context.Background()
	list := &ListResult{client: c, catalog: "users", Entries: []index.DeltaInfo{{Score: 42}}}

	var runs atomic.Int64
	sampler := NewSampler[int]("views", func(*ListResult) (int, error) {
		return int(runs.Add(1)), nil
	})

	if v, err := sampler.Sample(ctx, list); err != nil || v != 1 {
		t.Fatalf("first Sample: v=%d err=%v, want 1/nil", v, err)
	}
	if v, err := sampler.Sample(ctx, list); err != nil || v != 1 {
		t.Fatalf("cached Sample: v=%d err=%v, want 1/nil (loader must not re-run)", v, err)
	}

	n, err := c.InvalidateSamples(ctx, "views", "users")
	if err != nil {
		t.Fatalf("InvalidateSamples: %v", err)
	}
	if n != 1 {
		t.Fatalf("InvalidateSamples removed %d entries, want 1", n)
	}

	if v, err := sampler.Sample(ctx, list); err != nil || v != 2 {
		t.Fatalf("Sample after invalidate: v=%d err=%v, want 2/nil (recompute)", v, err)
	}
}
