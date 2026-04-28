package cache

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// CacheStat tracks cache statistics and periodically logs them. Close stops
// the background logger.
type CacheStat struct {
	name         string
	hit          uint64
	miss         uint64
	sizeCallback func() int
	done         chan struct{}
	closeOnce    sync.Once
}

// NewCacheStat creates a new cache stat tracker and starts the periodic
// logger.
func NewCacheStat(name string, sizeCallback func() int) *CacheStat {
	st := &CacheStat{
		name:         name,
		sizeCallback: sizeCallback,
		done:         make(chan struct{}),
	}
	go st.statLoop()
	return st
}

// Close stops the background logger. Idempotent.
func (cs *CacheStat) Close() error {
	cs.closeOnce.Do(func() {
		close(cs.done)
	})
	return nil
}

// IncrementHit increments hit counter.
func (cs *CacheStat) IncrementHit() {
	atomic.AddUint64(&cs.hit, 1)
}

// IncrementMiss increments miss counter.
func (cs *CacheStat) IncrementMiss() {
	atomic.AddUint64(&cs.miss, 1)
}

// statLoop periodically logs cache statistics until Close is called.
func (cs *CacheStat) statLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-cs.done:
			return
		case <-ticker.C:
			hit := atomic.SwapUint64(&cs.hit, 0)
			miss := atomic.SwapUint64(&cs.miss, 0)
			total := hit + miss
			if total == 0 {
				continue
			}
			percent := 100 * float32(hit) / float32(total)
			size := 0
			if cs.sizeCallback != nil {
				size = cs.sizeCallback()
			}
			log.Printf("[Lake Cache %s] qpm: %d, hit_ratio: %.1f%%, elements: %d, hit: %d, miss: %d",
				cs.name, total, percent, size, hit, miss)
		}
	}
}
