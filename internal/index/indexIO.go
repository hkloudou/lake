package index

import (
	"fmt"

	"github.com/hkloudou/lake/v3/internal/encode"
)

type indexIO struct {
	prefix string
}

func (w *indexIO) MakeDeltaZsetKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:%s:delta", w.prefix, encode.EncodeRedisCatalogName(catalog))
}

// makeSnapKey generates the Redis key for catalog snapshot index
// Kept in sync with Reader.makeSnapKey in keys.go
func (w *indexIO) MakeSnapZsetKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:%s:snap", w.prefix, encode.EncodeRedisCatalogName(catalog))
}

// MakeSampleIndicatorKey returns the Redis Hash key holding all sample
// results for the given indicator across catalogs.
//
// Layout: "<prefix>:samples:<indicator>". Each catalog is stored as a field
// of the hash, with [score, data] as the value. All catalogs sharing one
// indicator are colocated, so indicator-wide operations (clear, enumerate)
// stay single-key.
//
// Note: unlike the other Make*Key helpers, the parameter here is an indicator,
// not a catalog. The indicator is the inversion axis of the V2 sample layout.
func (w *indexIO) MakeSampleIndicatorKey(indicator string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:samples:%s", w.prefix, encode.EncodeRedisCatalogName(indicator))
}

// SetPrefix sets the key prefix (e.g., "oss:my-lake")
func (w *indexIO) SetPrefix(prefix string) {
	w.prefix = prefix
}

// Prefix returns the current key prefix
func (w *indexIO) Prefix() string {
	return w.prefix
}
