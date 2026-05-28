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

// TestSampleCacheCodec round-trips the [score, updatedAt, data] format and
// confirms the legacy 2-element [score, data] format fails to decode (so a
// reader treats it as a miss and recomputes).
func TestSampleCacheCodec(t *testing.T) {
	raw, err := marshalSampleCache(SampleMeta{Score: 12.5, UpdatedAt: 1700}, map[string]int{"a": 1})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	meta, data, err := unmarshalSampleCache[map[string]int](raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if meta.Score != 12.5 || meta.UpdatedAt != 1700 {
		t.Fatalf("meta round-trip: got %+v", meta)
	}
	if data["a"] != 1 {
		t.Fatalf("data round-trip: got %+v", data)
	}

	// Legacy 2-element format must NOT decode as a valid 3-element entry.
	if _, _, err := unmarshalSampleCache[map[string]int]([]byte(`[12.5,{"a":1}]`)); err == nil {
		t.Fatal("legacy 2-element cache value must fail to decode (forces recompute)")
	}
}

// TestSamplerIsStale locks in the layered staleness contract: the data-
// version floor is mandatory, while maxAge and shouldRefresh can only ADD
// refresh triggers — never suppress one the data version requires.
func TestSamplerIsStale(t *testing.T) {
	base := NewSampler[int]("x", func(*ListResult) (int, error) { return 0, nil })

	// Floor satisfied: cached version >= current, positive → fresh.
	if base.isStale(SampleMeta{Score: 100}, listAt(100), 0) {
		t.Error("equal version should be fresh")
	}
	// Data advanced → stale regardless of anything else.
	if !base.isStale(SampleMeta{Score: 100}, listAt(200), 0) {
		t.Error("advanced data version must be stale")
	}
	// Sentinel score 0 is never a valid hit (matches the score>0 rule).
	if !base.isStale(SampleMeta{Score: 0}, listAt(0), 0) {
		t.Error("zero score must be stale")
	}

	// maxAge: fresh within the window, stale past it.
	aged := NewSampler[int]("x", base.loader, WithMaxAge[int](10*time.Second))
	if aged.isStale(SampleMeta{Score: 100, UpdatedAt: 1000}, listAt(100), 1005) {
		t.Error("within maxAge should be fresh")
	}
	if !aged.isStale(SampleMeta{Score: 100, UpdatedAt: 1000}, listAt(100), 1015) {
		t.Error("past maxAge should be stale")
	}

	// shouldRefresh is additive: true forces a refresh even when fresh...
	force := NewSampler[int]("x", base.loader, WithShouldRefresh[int](
		func(SampleMeta, *ListResult) bool { return true }))
	if !force.isStale(SampleMeta{Score: 100}, listAt(100), 0) {
		t.Error("shouldRefresh=true must force a refresh")
	}
	// ...and false cannot suppress the data-version floor.
	keep := NewSampler[int]("x", base.loader, WithShouldRefresh[int](
		func(SampleMeta, *ListResult) bool { return false }))
	if !keep.isStale(SampleMeta{Score: 100}, listAt(200), 0) {
		t.Error("shouldRefresh=false must not override the data-version floor")
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
