package cache

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// CacheStat counts hits/misses and logs a one-line summary every 10s.
// Close stops the logger.
type CacheStat struct {
	name         string
	hit, miss    uint64
	sizeCallback func() int
	done         chan struct{}
	closeOnce    sync.Once
}

func NewCacheStat(name string, size func() int) *CacheStat {
	s := &CacheStat{name: name, sizeCallback: size, done: make(chan struct{})}
	go s.loop()
	return s
}

func (s *CacheStat) Close() error {
	s.closeOnce.Do(func() { close(s.done) })
	return nil
}

func (s *CacheStat) IncrementHit()  { atomic.AddUint64(&s.hit, 1) }
func (s *CacheStat) IncrementMiss() { atomic.AddUint64(&s.miss, 1) }

func (s *CacheStat) loop() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			hit := atomic.SwapUint64(&s.hit, 0)
			miss := atomic.SwapUint64(&s.miss, 0)
			total := hit + miss
			if total == 0 {
				continue
			}
			size := 0
			if s.sizeCallback != nil {
				size = s.sizeCallback()
			}
			log.Printf("[Lake Cache %s] qpm: %d, hit_ratio: %.1f%%, elements: %d, hit: %d, miss: %d",
				s.name, total, 100*float32(hit)/float32(total), size, hit, miss)
		}
	}
}
