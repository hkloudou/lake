package lake

// EventHandler is invoked around Lake operations for logging / monitoring.
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
