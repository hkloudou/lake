package lake

import (
	"context"
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
	// Unreachable Redis → ensureInitialized always fails → exercises the
	// "emit before early return" contract for every API.
	c := NewLake("127.0.0.1:1")
	t.Cleanup(func() { _ = c.Close() })
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
		t.Fatal("List event must be emitted even when ensureInitialized fails")
	}
}

func TestEmit_BatchListFiresOnInitFailure(t *testing.T) {
	c, spy := newClientWithSpy(t)

	c.BatchList(context.Background(), []string{"a", "b"})
	if !spy.seen("BatchList") {
		t.Fatal("BatchList event must be emitted even when ensureInitialized fails")
	}
}

func TestEmit_WriteFiresOnPathValidationFailure(t *testing.T) {
	c, spy := newClientWithSpy(t)

	// Invalid path: missing leading slash. Fails before init.
	_ = c.Write(context.Background(), WriteRequest{
		Catalog:   "users",
		Path:      "no-leading-slash",
		Body:      []byte(`{}`),
		MergeType: MergeTypeReplace,
	})
	if !spy.seen("Write") {
		t.Fatal("Write event must be emitted even when path validation fails")
	}
}

func TestEmit_ClearHistoryFiresOnInitFailure(t *testing.T) {
	c, spy := newClientWithSpy(t)

	_ = c.ClearHistory(context.Background(), "users")
	if !spy.seen("ClearHistory") {
		t.Fatal("ClearHistory event must be emitted even when ensureInitialized fails")
	}
}

func TestEmit_SampleFiresOnPendingShortCircuit(t *testing.T) {
	c, spy := newClientWithSpy(t)

	// Hand-craft a ListResult with HasPending=true so Sample short-circuits
	// before doing any Redis work; emit must still fire.
	list := &ListResult{
		client:     c,
		catalog:    "users",
		HasPending: true,
	}
	_, err := Sample[map[string]any](
		context.Background(), list, "report",
		func(*ListResult) (map[string]any, error) { return nil, nil },
	)
	if err == nil {
		t.Fatal("expected ErrPendingWrites, got nil")
	}
	if !spy.seen("Sample") {
		t.Fatal("Sample event must be emitted even when pending writes short-circuit")
	}
}
