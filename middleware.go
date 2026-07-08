package lake

// EventHandler is invoked around Lake operations for logging / monitoring.
//
// Handlers must be safe for concurrent use and should be fast and not panic:
// some events fire from Lake-internal goroutines — Batch loader workers
// (SampleCacheError) and the async snapshot saver (SnapshotError), the latter
// possibly after the triggering Read has already returned.
type EventHandler func(catalog, event string, attrs map[string]any)

// Use registers an event handler; handlers run in registration order. The
// handler list is copy-on-write behind an atomic pointer, so Use is safe to
// call at any time — including on a live Client — and emitEvent stays a
// single atomic load on the hot path.
func (c *Client) Use(h EventHandler) {
	c.useMu.Lock()
	defer c.useMu.Unlock()
	var next []EventHandler
	if old := c.eventHandlers.Load(); old != nil {
		next = append(next, *old...)
	}
	next = append(next, h)
	c.eventHandlers.Store(&next)
}

// hasHandlers is the hot-path guard call sites use to skip building the
// attrs map entirely when nobody is listening (the common production case).
func (c *Client) hasHandlers() bool {
	hs := c.eventHandlers.Load()
	return hs != nil && len(*hs) > 0
}

func (c *Client) emitEvent(catalog, event string, attrs map[string]any) {
	hs := c.eventHandlers.Load()
	if hs == nil {
		return
	}
	for _, h := range *hs {
		h(catalog, event, attrs)
	}
}
