package index

import (
	"fmt"

	"github.com/hkloudou/lake/v2/internal/encode"
)

type indexIO struct {
	prefix string
}

func (w *indexIO) MakeFileHmacKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:%s:files", w.prefix, encode.EncodeRedisCatalogName(catalog))
}

func (w *indexIO) MakeDeltaZsetKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:%s:delta", w.prefix, encode.EncodeRedisCatalogName(catalog))
}

func (w *indexIO) MakeMetaKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:%s:meta", w.prefix, encode.EncodeRedisCatalogName(catalog))
}

// makeSnapKey generates the Redis key for catalog snapshot index
// Kept in sync with Reader.makeSnapKey in keys.go
func (w *indexIO) MakeSnapZsetKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:%s:snap", w.prefix, encode.EncodeRedisCatalogName(catalog))
}

func (w *indexIO) makeSampleKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:%s:sample", w.prefix, encode.EncodeRedisCatalogName(catalog))
}

// SetPrefix sets the key prefix (e.g., "oss:my-lake")
func (w *indexIO) SetPrefix(prefix string) {
	w.prefix = prefix
}
