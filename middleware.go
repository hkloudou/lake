package lake

// EventHandler is a callback function for Lake operation events.
// catalog is the target catalog name.
// event is the operation name (e.g., "List", "Write", "BatchList").
// attrs provides operation-specific key-value pairs.
type EventHandler func(catalog string, event string, attrs map[string]any)

// Use registers an event handler on the Client.
// The handler is called around Lake operations for logging/monitoring.
func (c *Client) Use(handler EventHandler) {
	c.eventHandlers = append(c.eventHandlers, handler)
}

func (c *Client) emitEvent(catalog string, event string, attrs map[string]any) {
	for _, handler := range c.eventHandlers {
		handler(catalog, event, attrs)
	}
}
