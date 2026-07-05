package xsync

import (
	"fmt"
	"sync"
)

// SingleFlight dedupes concurrent calls under the same key — only one
// invocation of fn runs at a time per key; waiters share its result. If fn
// does not complete normally (it panics, or exits via runtime.Goexit), the
// abnormal exit keeps propagating in the leader caller and waiters receive
// an explicit error.
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

	// Cleanup must run even if fn never returns (panic, or runtime.Goexit —
	// e.g. a t.Fatalf inside a loader): otherwise the key stays in the map
	// and every future waiter blocks on wg forever. In both abnormal cases
	// waiters get an explicit error — NOT (zero, nil), which would be
	// indistinguishable from a successful call that produced the zero value
	// (e.g. an empty cache entry). The completed flag, not recover(), detects
	// this: recover() cannot see a Goexit, and not recovering lets the panic
	// keep propagating in this (leader) caller unaltered.
	completed := false
	defer func() {
		if !completed {
			c.err = fmt.Errorf("xsync: singleflight leader did not complete (panic or runtime.Goexit)")
		}
		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()
		c.wg.Done()
	}()

	c.val, c.err = fn()
	completed = true
	return c.val, c.err
}
