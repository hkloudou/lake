package lake

import (
	"errors"
	"testing"
	"time"

	"github.com/hkloudou/lake/v3/internal/index"
)

// listAt builds a ListResult whose LastUpdated() == score.
func listAt(score float64) *ListResult {
	return &ListResult{Entries: []index.DeltaInfo{{Score: score}}}
}

// TestSampleCacheCodec round-trips the [score, updatedAt, removeGen, data]
// format and confirms the legacy shorter formats fail to decode (so a
// reader treats them as a miss and recomputes).
func TestSampleCacheCodec(t *testing.T) {
	raw, err := marshalSampleCache(SampleMeta{Score: 12.5, UpdatedAt: 1700, RemoveGen: "3"}, map[string]int{"a": 1})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	meta, data, err := unmarshalSampleCache[map[string]int](raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if meta.Score != 12.5 || meta.UpdatedAt != 1700 || meta.RemoveGen != "3" {
		t.Fatalf("meta round-trip: got %+v", meta)
	}
	if data["a"] != 1 {
		t.Fatalf("data round-trip: got %+v", data)
	}

	// An empty generation is stored normalized, so a decoded entry always
	// compares equal against a no-removals ListResult.
	raw, _ = marshalSampleCache(SampleMeta{Score: 1, UpdatedAt: 1}, 0)
	if meta, _, err := unmarshalSampleCache[int](raw); err != nil || meta.RemoveGen != "0" {
		t.Fatalf("normalized gen: meta=%+v err=%v, want RemoveGen \"0\"", meta, err)
	}

	// Legacy 2- and 3-element formats must NOT decode as valid entries.
	for _, legacy := range []string{`[12.5,{"a":1}]`, `[12.5,1700,{"a":1}]`} {
		if _, _, err := unmarshalSampleCache[map[string]int]([]byte(legacy)); err == nil {
			t.Fatalf("legacy cache value %s must fail to decode (forces recompute)", legacy)
		}
	}
}

// TestNewSamplerValidatesIndicator: the indicator is embedded verbatim in
// the Redis memo key "<prefix>:m:<indicator>", so NewSampler enforces the
// catalog charset on it (fail-fast panic, per package policy).
func TestNewSamplerValidatesIndicator(t *testing.T) {
	loader := func(*ListResult) (int, error) { return 0, nil }

	for _, ok := range []string{"x", "user-stats", "a/b", "A.B_c"} {
		NewSampler[int](ok, loader) // must not panic
	}
	for _, bad := range []string{"a:b", "a|b", "(paren", "has space", "中文"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("indicator %q: expected panic, got none", bad)
				}
			}()
			NewSampler[int](bad, loader)
		}()
	}
}

// TestSamplerIsStale locks in the layered staleness contract: the data-
// version floor is mandatory, while maxAge and shouldRefresh can only ADD
// refresh triggers — never suppress one the data version requires.
func TestSamplerIsStale(t *testing.T) {
	base := NewSampler[int]("x", func(*ListResult) (int, error) { return 0, nil })
	noPeers := map[string]*ListResult(nil) // tests don't exercise peers here

	// Floor satisfied: cached version >= current, positive → fresh.
	if base.isStale(SampleMeta{Score: 100}, listAt(100), noPeers, 0) {
		t.Error("equal version should be fresh")
	}
	// Data advanced → stale regardless of anything else.
	if !base.isStale(SampleMeta{Score: 100}, listAt(200), noPeers, 0) {
		t.Error("advanced data version must be stale")
	}
	// Empty catalog: a sample computed at version 0 for a catalog still at
	// version 0 is a valid hit — otherwise every pre-provisioned empty
	// catalog would re-run its loader (and re-write the memo) on every call.
	// The removal-generation check still guards a catalog emptied by
	// RemoveDelta, and the first real write (version > 0) invalidates.
	if base.isStale(SampleMeta{Score: 0}, listAt(0), noPeers, 0) {
		t.Error("zero score for a still-empty catalog must be a hit")
	}
	// But a zero/garbage score never serves once the catalog has data.
	if !base.isStale(SampleMeta{Score: 0}, listAt(100), noPeers, 0) {
		t.Error("zero score with data present must be stale")
	}
	if !base.isStale(SampleMeta{Score: -1}, listAt(0), noPeers, 0) {
		t.Error("negative (corrupt) score must be stale")
	}
	// Removal-generation mismatch is mandatory staleness in BOTH directions:
	// the entry was not computed from the caller's view of the log.
	if !base.isStale(SampleMeta{Score: 100, RemoveGen: "1"}, listAt(100), noPeers, 0) {
		t.Error("entry from an older list generation must be stale")
	}
	genList := listAt(100)
	genList.removeGen = "2"
	if !base.isStale(SampleMeta{Score: 100, RemoveGen: "1"}, genList, noPeers, 0) {
		t.Error("generation mismatch must be stale")
	}
	genList.removeGen = "1"
	if base.isStale(SampleMeta{Score: 100, RemoveGen: "1"}, genList, noPeers, 0) {
		t.Error("matching generations must be fresh")
	}

	// maxAge: fresh within the window, stale past it.
	aged := NewSampler[int]("x", base.loader, WithMaxAge[int](10*time.Second))
	if aged.isStale(SampleMeta{Score: 100, UpdatedAt: 1000}, listAt(100), noPeers, 1005) {
		t.Error("within maxAge should be fresh")
	}
	if !aged.isStale(SampleMeta{Score: 100, UpdatedAt: 1000}, listAt(100), noPeers, 1015) {
		t.Error("past maxAge should be stale")
	}

	// Sub-second maxAge must not truncate to "always stale": with the clock at
	// the same second as the compute, the entry is younger than maxAge → fresh.
	subSec := NewSampler[int]("x", base.loader, WithMaxAge[int](500*time.Millisecond))
	if subSec.isStale(SampleMeta{Score: 100, UpdatedAt: 1000}, listAt(100), noPeers, 1000) {
		t.Error("sub-second maxAge with zero elapsed must be fresh")
	}
	if !subSec.isStale(SampleMeta{Score: 100, UpdatedAt: 1000}, listAt(100), noPeers, 1001) {
		t.Error("sub-second maxAge past one elapsed second must be stale")
	}

	// shouldRefresh is additive: true forces a refresh even when fresh...
	force := NewSampler[int]("x", base.loader, WithShouldRefresh[int](
		func(SampleMeta, *ListResult, map[string]*ListResult) bool { return true }))
	if !force.isStale(SampleMeta{Score: 100}, listAt(100), noPeers, 0) {
		t.Error("shouldRefresh=true must force a refresh")
	}
	// ...and false cannot suppress the data-version floor.
	keep := NewSampler[int]("x", base.loader, WithShouldRefresh[int](
		func(SampleMeta, *ListResult, map[string]*ListResult) bool { return false }))
	if !keep.isStale(SampleMeta{Score: 100}, listAt(200), noPeers, 0) {
		t.Error("shouldRefresh=false must not override the data-version floor")
	}
}

// TestSamplerCrossCatalogRefresh: the shouldRefresh predicate can read
// a peer's LastUpdated() and force a recompute when the peer has moved
// past the version the sample recorded as its dependency baseline.
func TestSamplerCrossCatalogRefresh(t *testing.T) {
	// Sentinel for "the sample was computed assuming peer B at version 50".
	const baselineB = 50.0

	sampler := NewSampler[int]("x",
		func(*ListResult) (int, error) { return 0, nil },
		WithShouldRefresh[int](func(_ SampleMeta, _ *ListResult, peers map[string]*ListResult) bool {
			b := peers["B"]
			return b != nil && b.LastUpdated() > baselineB
		}),
	)
	meta := SampleMeta{Score: 100} // self's data version unchanged

	// Peer B still at baseline → fresh.
	peers := map[string]*ListResult{"A": listAt(100), "B": listAt(baselineB)}
	if sampler.isStale(meta, peers["A"], peers, 0) {
		t.Error("peer at baseline must NOT trigger refresh")
	}

	// Peer B advanced → predicate fires, refresh required.
	peers["B"] = listAt(baselineB + 1)
	if !sampler.isStale(meta, peers["A"], peers, 0) {
		t.Error("peer past baseline MUST trigger refresh")
	}

	// Peer absent from batch context: predicate can't see B → no refresh.
	// Caller is responsible for including B in BatchList if they want
	// this dependency evaluated.
	peers = map[string]*ListResult{"A": listAt(100)}
	if sampler.isStale(meta, peers["A"], peers, 0) {
		t.Error("absent peer must not trigger refresh (predicate sees nil)")
	}
}

// TestSamplerFinalize verifies the error-handling contract: a loader error
// is unwrapped (so caller errors.Is works), a fallback substitutes only
// when it accepts the error, and non-loader (internal) errors propagate.
func TestSamplerFinalize(t *testing.T) {
	sentinel := errors.New("db down")

	// No fallback: the original loader error surfaces, unwrapped.
	plain := NewSampler[int]("x", func(*ListResult) (int, error) { return 0, nil })
	if _, err := plain.finalize(0, &loaderError{err: sentinel}); !errors.Is(err, sentinel) {
		t.Fatalf("expected unwrapped sentinel, got %v", err)
	}

	// Fallback default: substitutes a value, drops the error.
	def := NewSampler[int]("x", plain.loader, WithLoaderErrorDefault[int](42))
	if v, err := def.finalize(0, &loaderError{err: sentinel}); err != nil || v != 42 {
		t.Fatalf("expected (42, nil), got (%d, %v)", v, err)
	}

	// Fallback that declines (ok=false) propagates the unwrapped error.
	decline := NewSampler[int]("x", plain.loader, WithLoaderErrorFallback[int](
		func(error) (int, bool) { return 0, false }))
	if _, err := decline.finalize(0, &loaderError{err: sentinel}); !errors.Is(err, sentinel) {
		t.Fatalf("declined fallback must propagate sentinel, got %v", err)
	}

	// A non-loader (internal) error is never eligible for the fallback.
	internal := errors.New("marshal sample: boom")
	if _, err := def.finalize(0, internal); !errors.Is(err, internal) {
		t.Fatalf("internal error must propagate as-is, got %v", err)
	}

	// Happy path is a passthrough.
	if v, err := def.finalize(7, nil); err != nil || v != 7 {
		t.Fatalf("expected (7, nil), got (%d, %v)", v, err)
	}
}
