package index

import (
	"fmt"

	"github.com/hkloudou/lake/v2/internal/encode"
)

type indexIO struct {
	prefix string
}

func (w *indexIO) makeDeltaZsetKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:delta:%s", w.prefix, encode.EncodeRedisCatalogName(catalog))
}

func (w *indexIO) makeMetaKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:meta:%s", w.prefix, encode.EncodeRedisCatalogName(catalog))
}

// makeSnapKey generates the Redis key for catalog snapshot index
// Kept in sync with Reader.makeSnapKey in keys.go
func (w *indexIO) makeSnapKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:snap:%s", w.prefix, encode.EncodeRedisCatalogName(catalog))
}

// SetPrefix sets the key prefix (e.g., "oss:my-lake")
func (w *indexIO) SetPrefix(prefix string) {
	w.prefix = prefix
}
