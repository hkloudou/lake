package lake

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestClientCloseDrainsAllGoroutines verifies that Client.Close stops all
// background goroutines spawned by NewLake — Reader's time updater,
// MemoryCache cleanup, and the snap RedisCache's stat logger.
//
// We point at an unreachable Redis to avoid requiring a real server; the
// background goroutines start regardless and must still exit on Close.
func TestClientCloseDrainsAllGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	c := NewLake("127.0.0.1:1") // unreachable; ok for this test

	// Let goroutines start (Reader updater + MemoryCache cleanup + RedisCache stat).
	time.Sleep(30 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after < before+3 {
		t.Fatalf("expected at least 3 new goroutines after NewLake; before=%d after=%d", before, after)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	// Idempotent.
	if err := c.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}

	// The updater goroutine may be mid-Dial against the unreachable address
	// when Close is called; give it a generous window to observe `done`
	// after the in-flight Dial fails.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutines did not drain within 2s; final=%d before=%d", runtime.NumGoroutine(), before)
}

// TestClientCloseIdempotentConcurrent stresses Close called from many
// goroutines simultaneously; missing closeOnce would panic via close()
// on a closed channel.
func TestClientCloseIdempotentConcurrent(t *testing.T) {
	c := NewLake("127.0.0.1:1")

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
