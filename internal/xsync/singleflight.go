package xsync

import "sync"

type (
	// SingleFlight prevents duplicate function execution for the same key
	SingleFlight[T any] interface {
		Do(key string, fn func() (T, error)) (T, error)
	}

	call[T any] struct {
		wg  sync.WaitGroup
		val T
		err error
	}

	flightGroup[T any] struct {
		calls map[string]*call[T]
		lock  sync.Mutex
	}
)

// NewSingleFlight creates a new SingleFlight instance
func NewSingleFlight[T any]() SingleFlight[T] {
	return &flightGroup[T]{
		calls: make(map[string]*call[T]),
	}
}

func (g *flightGroup[T]) Do(key string, fn func() (T, error)) (T, error) {
	c, done := g.createCall(key)
	if done {
		return c.val, c.err
	}

	g.makeCall(c, key, fn)
	return c.val, c.err
}

func (g *flightGroup[T]) createCall(key string) (c *call[T], done bool) {
	g.lock.Lock()
	if c, ok := g.calls[key]; ok {
		g.lock.Unlock()
		c.wg.Wait()
		return c, true
	}

	c = new(call[T])
	c.wg.Add(1)
	g.calls[key] = c
	g.lock.Unlock()

	return c, false
}

func (g *flightGroup[T]) makeCall(c *call[T], key string, fn func() (T, error)) {
	defer func() {
		g.lock.Lock()
		delete(g.calls, key)
		g.lock.Unlock()
		c.wg.Done()
	}()

	c.val, c.err = fn()
}

