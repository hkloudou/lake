package lake

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// Event contract under test: emitEvent fires before any early return,
// including initialization failure (lake.setting unreachable). Without
// this guarantee, EventHandlers cannot reliably observe every API call
// attempt, which breaks audit / metrics / tracing use cases.

type spyHandler struct {
	mu     sync.Mutex
	events []string
}

func (s *spyHandler) handler() EventHandler {
	return func(catalog, event string, attrs map[string]any) {
		s.mu.Lock()
		s.events = append(s.events, event)
		s.mu.Unlock()
	}
}

func (s *spyHandler) seen(event string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.events {
		if e == event {
			return true
		}
	}
	return false
}

func newClientWithSpy(t *testing.T) (*Client, *spyHandler) {
	t.Helper()
	// Unreachable Redis → operations fail past the event emit → exercises the
	// "emit before early return" contract for every API.
	c := newDeadClient(t)
	spy := &spyHandler{}
	c.Use(spy.handler())
	return c, spy
}

func TestEmit_ListFiresOnInitFailure(t *testing.T) {
	c, spy := newClientWithSpy(t)

	res := c.List(context.Background(), "users")
	if res.Err == nil {
		t.Fatalf("expected init failure, got nil error")
	}
	if !spy.seen("List") {
		t.Fatal("List event must be emitted even when the Redis read fails")
	}
}

func TestEmit_BatchListFiresOnInitFailure(t *testing.T) {
	c, spy := newClientWithSpy(t)

	c.BatchList(context.Background(), []string{"a", "b"})
	if !spy.seen("BatchList") {
		t.Fatal("BatchList event must be emitted even when the Redis read fails")
	}
}

func TestEmit_WriteBeginFiresOnPathValidationFailure(t *testing.T) {
	c, spy := newClientWithSpy(t)

	// Invalid path: missing leading slash. Fails before init.
	_, _ = c.WriteBegin(context.Background(), WriteBeginRequest{
		Catalog:   "users",
		Path:      "no-leading-slash",
		MergeType: MergeTypeReplace,
	})
	if !spy.seen("WriteBegin") {
		t.Fatal("WriteBegin event must be emitted even when path validation fails")
	}
}

func TestEmit_SampleFiresOnListErr(t *testing.T) {
	c, spy := newClientWithSpy(t)

	// Hand-craft a ListResult with Err so Sample short-circuits;
	// emit must still fire.
	list := &ListResult{client: c, catalog: "users", Err: errIntentional}
	sampler := NewSampler[map[string]any]("report",
		func(*ListResult) (map[string]any, error) { return nil, nil })
	_, err := sampler.Sample(context.Background(), list)
	if err == nil {
		t.Fatal("expected list-err propagation, got nil")
	}
	if !spy.seen("Sample") {
		t.Fatal("Sample event must be emitted even when list.Err short-circuits")
	}
}

var errIntentional = errors.New("intentional")
