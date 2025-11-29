package lake

import (
	"context"
)

func (c *Client) Meta(ctx context.Context, catalog string) (string, error) {
	if err := c.ensureInitialized(ctx); err != nil {
		return "", err
	}
	return c.reader.Meta(ctx, catalog)
}
