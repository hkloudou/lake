package index

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestReaderCloseStopsUpdater verifies that Reader.Close terminates the
// background time-sync goroutine and is idempotent.
//
// We construct a Reader pointed at an unreachable Redis address; the
// updater goroutine must spin getTimeUnix → error → ticker.C, but Close
// must still cause it to exit promptly.
func TestReaderCloseStopsUpdater(t *testing.T) {
	before := runtime.NumGoroutine()

	rdb := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1", // intentionally unreachable
		DialTimeout: 50 * time.Millisecond,
	})
	t.Cleanup(func() { _ = rdb.Close() })

	r := NewReader(rdb)

	// Let the updater start. It will fail to reach Redis, which is fine.
	time.Sleep(20 * time.Millisecond)
	if runtime.NumGoroutine() <= before {
		t.Fatalf("expected at least one new goroutine after NewReader; before=%d after=%d", before, runtime.NumGoroutine())
	}

	r.Close()
	r.Close() // idempotent

	if !waitForGoroutineDrop(before+1, 500*time.Millisecond) {
		t.Fatalf("updater goroutine did not exit within 500ms; goroutines=%d before=%d", runtime.NumGoroutine(), before)
	}
}

// TestReaderCloseConcurrent stresses Close against itself; close(done)
// panics on double-close, so this catches a missing closeOnce.
func TestReaderCloseConcurrent(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 50 * time.Millisecond,
	})
	t.Cleanup(func() { _ = rdb.Close() })

	r := NewReader(rdb)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Close()
		}()
	}
	wg.Wait()
}

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
