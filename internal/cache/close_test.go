package cache

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestMemoryCacheCloseStopsCleanupLoop verifies that Close terminates
// the background cleanup goroutine and is idempotent.
func TestMemoryCacheCloseStopsCleanupLoop(t *testing.T) {
	before := runtime.NumGoroutine()

	c := NewMemoryCache(1 * time.Minute)

	// Give the cleanup goroutine a moment to start.
	time.Sleep(20 * time.Millisecond)
	if runtime.NumGoroutine() <= before {
		t.Fatalf("expected at least one new goroutine after NewMemoryCache; before=%d after=%d", before, runtime.NumGoroutine())
	}

	if err := c.Close(); err != nil {
		t.Fatalf("first Close returned error: %v", err)
	}
	// Idempotent: a second Close must not panic or error.
	if err := c.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}

	// Allow the goroutine to observe done and exit.
	if !waitForGoroutineDrop(before+1, 500*time.Millisecond) {
		t.Fatalf("cleanup goroutine did not exit within 500ms; goroutines=%d before=%d", runtime.NumGoroutine(), before)
	}
}

// TestCacheStatCloseStopsStatLoop verifies the stat logger goroutine
// exits on Close and that Close is idempotent.
func TestCacheStatCloseStopsStatLoop(t *testing.T) {
	before := runtime.NumGoroutine()

	st := NewCacheStat("test", func() int { return 0 })

	time.Sleep(20 * time.Millisecond)
	if runtime.NumGoroutine() <= before {
		t.Fatalf("expected new goroutine after NewCacheStat; before=%d after=%d", before, runtime.NumGoroutine())
	}

	if err := st.Close(); err != nil {
		t.Fatalf("first Close returned error: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}

	if !waitForGoroutineDrop(before+1, 500*time.Millisecond) {
		t.Fatalf("stat goroutine did not exit within 500ms; goroutines=%d before=%d", runtime.NumGoroutine(), before)
	}
}

// TestMemoryCacheCloseConcurrent stresses the idempotency guarantee
// under racing Close callers — close(c.done) panics on double-close, so
// this catches bugs where closeOnce is not used correctly.
func TestMemoryCacheCloseConcurrent(t *testing.T) {
	c := NewMemoryCache(1 * time.Minute)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Close()
		}()
	}
	wg.Wait()
}

// waitForGoroutineDrop polls runtime.NumGoroutine until it falls to or below
// target, or timeout elapses.
func waitForGoroutineDrop(target int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= target {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return runtime.NumGoroutine() <= target
}
