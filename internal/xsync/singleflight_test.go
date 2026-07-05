package xsync

import (
	"errors"
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

// TestSingleFlight_PanicSurfacesToWaiters: a waiter joined to a leader whose fn
// panics must observe an ERROR — not (zero, nil), which would be
// indistinguishable from a successful call that produced the zero value (an
// empty cache entry, a zero sample). The panic itself still belongs to the
// leader only.
func TestSingleFlight_PanicSurfacesToWaiters(t *testing.T) {
	g := NewSingleFlight[int]()

	entered := make(chan struct{})
	release := make(chan struct{})
	go func() { // leader: panics once released
		defer func() { _ = recover() }()
		_, _ = g.Do("k", func() (int, error) {
			close(entered)
			<-release
			panic("boom")
		})
	}()
	<-entered

	type result struct {
		v   int
		err error
	}
	done := make(chan result, 1)
	go func() { // waiter: joins the in-flight call
		v, err := g.Do("k", func() (int, error) {
			t.Error("waiter must join the leader's flight, not run its own fn")
			return -1, nil
		})
		done <- result{v, err}
	}()
	// Give the waiter time to register on the in-flight call before the
	// leader panics; if it were somehow late, its own fn would t.Error above.
	time.Sleep(50 * time.Millisecond)
	close(release)

	res := <-done
	if res.err == nil || !strings.Contains(res.err.Error(), "panicked") {
		t.Fatalf("waiter got (%d, %v), want a leader-panicked error", res.v, res.err)
	}
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
