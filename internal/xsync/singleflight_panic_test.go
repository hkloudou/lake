package xsync

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestLeaderPanicWaitersGetError: a panicking leader must propagate its panic
// to itself, but waiters must observe a real error — never (zero, nil), which
// callers would treat as a successful empty result (e.g. a nil snapshot body
// merged and then persisted).
func TestLeaderPanicWaitersGetError(t *testing.T) {
	g := NewSingleFlight[[]byte]()

	leaderIn := make(chan struct{})
	release := make(chan struct{})

	var wg sync.WaitGroup
	wg.Go(func() {
		defer func() {
			if recover() == nil {
				t.Error("leader panic did not propagate")
			}
		}()
		g.Do("k", func() ([]byte, error) {
			close(leaderIn)
			<-release
			panic("boom")
		})
	})

	<-leaderIn
	waiterErr := make(chan error, 1)
	wg.Go(func() {
		v, err := g.Do("k", func() ([]byte, error) { return []byte("second"), nil })
		if err == nil && v == nil {
			waiterErr <- errors.New("waiter observed (nil, nil)")
			return
		}
		waiterErr <- err
	})

	// Give the waiter time to park on the flight, then let the leader blow up.
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	err := <-waiterErr
	if err != nil && !errors.Is(err, ErrLeaderPanicked) {
		t.Fatalf("waiter got %v, want ErrLeaderPanicked (or a fresh successful run)", err)
	}
	// err == nil is also acceptable ONLY if the waiter ran fn itself after
	// cleanup (it arrived post-delete and became a new leader) — that path
	// returns ("second", nil), which the check above admits.

	// The key must not be wedged for later callers.
	v, err := g.Do("k", func() ([]byte, error) { return []byte("after"), nil })
	if err != nil || string(v) != "after" {
		t.Fatalf("flight wedged after leader panic: v=%q err=%v", v, err)
	}
}
