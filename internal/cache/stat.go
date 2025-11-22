package cache

import (
	"log"
	"sync/atomic"
	"time"
)

// CacheStat tracks cache statistics
type CacheStat struct {
	name         string
	hit          uint64
	miss         uint64
	sizeCallback func() int
}

// NewCacheStat creates a new cache stat tracker
func NewCacheStat(name string, sizeCallback func() int) *CacheStat {
	st := &CacheStat{
		name:         name,
		sizeCallback: sizeCallback,
	}
	go st.statLoop()
	return st
}

// IncrementHit increments hit counter
func (cs *CacheStat) IncrementHit() {
	atomic.AddUint64(&cs.hit, 1)
}

// IncrementMiss increments miss counter
func (cs *CacheStat) IncrementMiss() {
	atomic.AddUint64(&cs.miss, 1)
}

// statLoop periodically logs cache statistics
func (cs *CacheStat) statLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
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

