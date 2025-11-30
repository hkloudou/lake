package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v2/internal/merge"
	"github.com/hkloudou/lake/v2/internal/utils"
	"github.com/hkloudou/lake/v2/trace"
	"github.com/tidwall/sjson"
)

// WriteRequest represents a write request
type WriteFileRequest struct {
	Catalog string // Catalog name
	Path    string // JSON path (e.g., "user.profile.name")
	Meta    []byte
	Body    []byte // JSON body to write (raw bytes from network)
}

func (c *Client) WriteFile(ctx context.Context, req WriteFileRequest) error {
	// shardedPath := encode.EncodeOssCatalogPath(catalog, 4) // Default: 4-char MD5 prefix (65,536 dirs)
	// return c.storage.Put(ctx, shardedPath+"/"+filename, data)
	tr := trace.FromContext(ctx)
	tr.RecordSpan("WriteFile.Init")

	tr.RecordSpan("WriteFile.ValidateFieldPath", map[string]any{
		"path":    req.Path,
		"catalog": req.Catalog,
		"meta":    string(req.Meta),
		"body":    len(req.Body),
	})

	if err := utils.ValidateFilePath(req.Path); err != nil {
		return err
	}

	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}

	storageFileKey := c.storage.MakeFileKey(req.Catalog, req.Path)
	if err := c.storage.Put(ctx, storageFileKey, req.Body); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	tr.RecordSpan("WriteFile.StoragePut", map[string]any{
		"key":  storageFileKey,
		"size": len(req.Body),
	})
	fileHmacKey := c.writer.MakeFileHmacKey(req.Catalog)

	if err := c.rdb.HSet(ctx, fileHmacKey, req.Path, req.Meta).Err(); err != nil {
		return fmt.Errorf("failed to write file hmac: %w", err)
	}
	tr.RecordSpan("WriteFile.RedisHSet", map[string]any{
		"key":  fileHmacKey,
		"size": len(req.Meta),
	})
	return nil
}

func (c *Client) FileExists(ctx context.Context, catalog string, path string) (bool, error) {
	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return false, err
	}
	fileHmacKey := c.writer.MakeFileHmacKey(catalog)
	if err := utils.ValidateFilePath(path); err != nil {
		return false, err
	}
	return c.rdb.HExists(ctx, fileHmacKey, path).Val(), nil
}

func (c *Client) FilesAndMeta(ctx context.Context, catalog string) (string, error) {
	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return "", err
	}
	fileHmacKey := c.writer.MakeFileHmacKey(catalog)
	fmt.Println("fileHmacKey", fileHmacKey)
	// HGetAll returns map[string]string
	result, err := c.rdb.HGetAll(ctx, fileHmacKey).Result()
	if err != nil {
		return "", fmt.Errorf("failed to get files and meta: %w", err)
	}

	// Deserialize JSON strings to map[string]any
	filesAndMeta := "{}"
	for field, jsonStr := range result {

		filesAndMeta, _ = sjson.SetRaw(filesAndMeta, merge.ToGjsonPath(field), jsonStr)
	}

	return filesAndMeta, nil
}
