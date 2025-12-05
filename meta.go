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

func (c *Client) BatchMeta(ctx context.Context, catalogs []string) (map[string]string, error) {
	if err := c.ensureInitialized(ctx); err != nil {
		return nil, err
	}
	return c.reader.BatchMeta(ctx, catalogs)
}
