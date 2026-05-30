package xsync

import (
	"errors"
	"testing"
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
