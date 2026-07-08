package xsync

import (
	"errors"
	"sync"
)

// ErrLeaderPanicked is what waiters observe when the flight leader's fn
// panicked: a real error, never a silent (zero value, nil) success — a nil
// []byte with a nil error would read as a legitimate empty result and
// propagate silently (e.g. an empty snapshot base).
var ErrLeaderPanicked = errors.New("xsync: singleflight leader panicked")

// SingleFlight dedupes concurrent calls under the same key — only one
// invocation of fn runs at a time per key; waiters share its result.
type SingleFlight[T any] interface {
	Do(key string, fn func() (T, error)) (T, error)
}

type call[T any] struct {
	wg  sync.WaitGroup
	val T
	err error
}

type flightGroup[T any] struct {
	mu    sync.Mutex
	calls map[string]*call[T]
}

func NewSingleFlight[T any]() SingleFlight[T] {
	return &flightGroup[T]{calls: make(map[string]*call[T])}
}

func (g *flightGroup[T]) Do(key string, fn func() (T, error)) (T, error) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &call[T]{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	// Cleanup must run even if fn panics: otherwise the key stays in the map
	// and every future waiter blocks on wg forever. The panic still propagates
	// to this (leader) caller; in-flight waiters observe ErrLeaderPanicked —
	// never a (zero, nil) that would read as success.
	finished := false
	defer func() {
		if !finished {
			c.err = ErrLeaderPanicked
		}
		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()
		c.wg.Done()
	}()

	c.val, c.err = fn()
	finished = true
	return c.val, c.err
}
