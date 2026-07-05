package lake

// EventHandler is invoked around Lake operations for logging / monitoring.
//
// Handlers must be safe for concurrent use and should be fast and not panic:
// some events fire from Lake-internal goroutines — Batch loader workers
// (SampleCacheError) and the async snapshot saver (SnapshotError), the latter
// possibly after the triggering Read has already returned.
type EventHandler func(catalog, event string, attrs map[string]any)

// Use registers an event handler; handlers run in registration order. Call it
// during setup, before the Client serves requests: Use mutates the handler slice
// without locking, so it is not safe to call concurrently with operations.
func (c *Client) Use(h EventHandler) {
	c.eventHandlers = append(c.eventHandlers, h)
}

func (c *Client) emitEvent(catalog, event string, attrs map[string]any) {
	for _, h := range c.eventHandlers {
		h(catalog, event, attrs)
	}
}
