package xsync

import "sync"

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

	c.val, c.err = fn()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	c.wg.Done()

	return c.val, c.err
}
