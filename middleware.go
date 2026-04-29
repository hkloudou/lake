package lake

// EventHandler is invoked around Lake operations for logging / monitoring.
type EventHandler func(catalog, event string, attrs map[string]any)

// Use registers an event handler. Handlers run in registration order.
func (c *Client) Use(h EventHandler) {
	c.eventHandlers = append(c.eventHandlers, h)
}

func (c *Client) emitEvent(catalog, event string, attrs map[string]any) {
	for _, h := range c.eventHandlers {
		h(catalog, event, attrs)
	}
}
