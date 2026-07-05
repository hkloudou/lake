package xsync

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestSingleFlight_PanicDoesNotWedgeKey is the regression guard: a fn that
// panics must still free its key (via the deferred cleanup), so a later Do with
// the same key runs fresh instead of blocking forever on the WaitGroup.
func TestSingleFlight_PanicDoesNotWedgeKey(t *testing.T) {
	g := NewSingleFlight[int]()

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic must propagate to the leader caller")
			}
		}()
		_, _ = g.Do("k", func() (int, error) { panic("boom") })
	}()

	// If the key were still held, this Do would deadlock on wg.Wait().
	got, err := g.Do("k", func() (int, error) { return 42, nil })
	if err != nil || got != 42 {
		t.Fatalf("after a panicking call, Do(k) = (%d, %v), want (42, nil)", got, err)
	}
}

// testAbnormalLeaderSurfacesErrorToWaiters: a waiter joined to a leader whose
// fn never returns (panic or runtime.Goexit) must observe an ERROR — not
// (zero, nil), which would be indistinguishable from a successful call that
// produced the zero value (an empty cache entry, a zero sample). The abnormal
// exit itself still belongs to the leader only.
//
// The waiter's registration cannot be observed from outside, so the test
// retries the interleaving with a fresh key until the waiter actually joins
// the in-flight call (detected via its fallback fn NOT running) — a lost race
// retries instead of flaking on a starved CI runner.
func testAbnormalLeaderSurfacesErrorToWaiters(t *testing.T, abort func()) {
	t.Helper()
	g := NewSingleFlight[int]()

	for attempt := 0; attempt < 100; attempt++ {
		key := fmt.Sprintf("k%d", attempt)
		entered := make(chan struct{})
		release := make(chan struct{})
		go func() { // leader: exits abnormally once released
			defer func() { _ = recover() }()
			_, _ = g.Do(key, func() (int, error) {
				close(entered)
				<-release
				abort()
				return 0, nil // unreachable
			})
		}()
		<-entered

		type result struct {
			v     int
			err   error
			ownFn bool
		}
		done := make(chan result, 1)
		go func() { // waiter: tries to join the in-flight call
			own := false
			v, err := g.Do(key, func() (int, error) {
				own = true // lost the race: the leader was already gone
				return -1, nil
			})
			done <- result{v, err, own}
		}()
		time.Sleep(10 * time.Millisecond) // best-effort; a missed join just retries
		close(release)

		res := <-done
		if res.ownFn {
			continue // waiter never joined this flight; retry with a fresh key
		}
		if res.err == nil || !strings.Contains(res.err.Error(), "did not complete") {
			t.Fatalf("waiter got (%d, %v), want a leader-did-not-complete error", res.v, res.err)
		}
		return
	}
	t.Fatal("waiter never joined the leader's flight in 100 attempts")
}

func TestSingleFlight_PanicSurfacesErrorToWaiters(t *testing.T) {
	testAbnormalLeaderSurfacesErrorToWaiters(t, func() { panic("boom") })
}

// runtime.Goexit is the t.Fatalf path: a loader calling t.Fatalf inside a
// test must not hand concurrent waiters a phantom (zero, nil) success.
func TestSingleFlight_GoexitSurfacesErrorToWaiters(t *testing.T) {
	testAbnormalLeaderSurfacesErrorToWaiters(t, runtime.Goexit)
}

// TestSingleFlight_ReturnsValueAndError covers the plain passthrough: Do returns
// fn's value and error unchanged.
func TestSingleFlight_ReturnsValueAndError(t *testing.T) {
	g := NewSingleFlight[string]()

	if v, err := g.Do("a", func() (string, error) { return "ok", nil }); v != "ok" || err != nil {
		t.Fatalf(`Do("a") = (%q, %v), want ("ok", nil)`, v, err)
	}

	sentinel := errors.New("boom")
	if v, err := g.Do("b", func() (string, error) { return "", sentinel }); v != "" || !errors.Is(err, sentinel) {
		t.Fatalf(`Do("b") = (%q, %v), want ("", boom)`, v, err)
	}
}
