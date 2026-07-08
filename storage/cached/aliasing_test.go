package cached

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

// TestMemoryCacheConcurrentTakeCopies: concurrent Take callers of the SAME
// key (flight leader and waiters alike) must each receive a private slice.
// Lake's read path documents that callers may mutate the merged document —
// which, for a fully-snapshotted catalog, IS the slice Take returned.
func TestMemoryCacheConcurrentTakeCopies(t *testing.T) {
	c := NewMemoryCache(time.Minute)
	defer c.Close()
	ctx := context.Background()

	const n = 8
	gate := make(chan struct{})
	results := make([][]byte, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Go(func() {
			<-gate
			v, err := c.Take(ctx, "ns", "k", func() ([]byte, error) {
				time.Sleep(30 * time.Millisecond) // widen the flight window
				return []byte(`{"doc":1}`), nil
			})
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = v
		})
	}
	close(gate)
	wg.Wait()

	for i, v := range results {
		if v == nil {
			t.Fatalf("result %d missing", i)
		}
		for j := i + 1; j < n; j++ {
			if results[j] != nil && &v[0] == &results[j][0] {
				t.Fatalf("results %d and %d share a backing array", i, j)
			}
		}
	}
	// Mutating one caller's document must not leak anywhere.
	results[0][0] = 'X'
	v, err := c.Take(ctx, "ns", "k", func() ([]byte, error) { return nil, nil })
	if err != nil || !bytes.Equal(v, []byte(`{"doc":1}`)) {
		t.Fatalf("cache content corrupted by caller mutation: %q err=%v", v, err)
	}
}

// TestMemoryCacheHitDoesNotSerialize: hits are served outside the
// single-flight — a slow loader for one key must not block hits already
// cached under the same key from other goroutines... more precisely, hits
// must proceed while a miss-flight for a DIFFERENT key is running, and
// repeated hits need no flight at all. We pin the observable part: a hit
// completes quickly while an unrelated loader is stuck.
func TestMemoryCacheHitDoesNotSerialize(t *testing.T) {
	c := NewMemoryCache(time.Minute)
	defer c.Close()
	ctx := context.Background()

	if err := c.Set(ctx, "ns", "hot", []byte("v")); err != nil {
		t.Fatal(err)
	}

	stuck := make(chan struct{})
	go c.Take(ctx, "ns", "cold", func() ([]byte, error) {
		<-stuck
		return []byte("cold"), nil
	})
	defer close(stuck)

	done := make(chan struct{})
	go func() {
		if _, err := c.Take(ctx, "ns", "hot", func() ([]byte, error) {
			t.Error("loader ran for a cached key")
			return nil, nil
		}); err != nil {
			t.Error(err)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("hit blocked behind an unrelated in-flight miss")
	}
}

// TestCacheContextCancelRetry: a waiter with a healthy context must not fail
// just because the flight LEADER's context was cancelled mid-load.
func TestCacheContextCancelRetry(t *testing.T) {
	c := NewMemoryCache(time.Minute)
	defer c.Close()

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderIn := make(chan struct{})

	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = c.Take(leaderCtx, "ns", "k", func() ([]byte, error) {
			close(leaderIn)
			<-leaderCtx.Done()
			return nil, leaderCtx.Err()
		})
	})

	<-leaderIn
	waiterDone := make(chan error, 1)
	wg.Go(func() {
		// Healthy-context waiter parks on the leader's flight.
		v, err := c.Take(context.Background(), "ns", "k", func() ([]byte, error) {
			return []byte("fresh"), nil
		})
		if err == nil && string(v) != "fresh" {
			waiterDone <- err
			return
		}
		waiterDone <- err
	})

	time.Sleep(20 * time.Millisecond)
	cancelLeader()
	wg.Wait()

	if err := <-waiterDone; err != nil {
		t.Fatalf("healthy waiter failed with the leader's cancellation: %v", err)
	}
}
