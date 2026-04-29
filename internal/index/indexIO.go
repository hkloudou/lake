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

// MakeSnapsHashKey returns the single Redis Hash that holds the latest
// snapshot for every catalog in this deployment.
//
// Layout: "<prefix>:snaps". Each catalog is a Hash field whose value is
// the snap encoded by EncodeSnapValue. One round-trip HMGet / HGETALL
// fetches every catalog's snap metadata, which is what makes whole-
// deployment backup and bulk inspection cheap.
//
// Note: unlike MakeMetaKey / MakeDeltaZsetKey (which take a catalog and
// return per-catalog keys), MakeSnapsHashKey is global. The catalog
// is the *field* inside this hash, not part of the key.
func (w *indexIO) MakeSnapsHashKey() string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:snaps", w.prefix)
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
