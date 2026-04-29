package index

import (
	"fmt"

	"github.com/hkloudou/lake/v3/internal/encode"
)

// indexIO holds the deployment-level Redis key prefix and renders the
// canonical Redis keys used by Reader / Writer. All keys live under
// "<prefix>:..." so different deployments sharing one Redis stay
// isolated by Name.
type indexIO struct {
	prefix string
}

func (w *indexIO) SetPrefix(p string) { w.prefix = p }
func (w *indexIO) Prefix() string     { return w.prefix }

func (w *indexIO) requirePrefix() {
	if w.prefix == "" {
		panic("index: prefix not set; call SetPrefix before use")
	}
}

// MakeDeltaZsetKey: per-catalog delta ZSet "<prefix>:<catalog>:delta".
func (w *indexIO) MakeDeltaZsetKey(catalog string) string {
	w.requirePrefix()
	return fmt.Sprintf("%s:%s:delta", w.prefix, encode.EncodeRedisCatalogName(catalog))
}

// MakeSnapsHashKey: deployment-wide snap Hash "<prefix>:snaps", with
// catalog as field. One HMGet/HGETALL surfaces every snap at once.
func (w *indexIO) MakeSnapsHashKey() string {
	w.requirePrefix()
	return fmt.Sprintf("%s:snaps", w.prefix)
}

// MakeSampleIndicatorKey: per-indicator sample Hash
// "<prefix>:samples:<indicator>", with catalog as field.
func (w *indexIO) MakeSampleIndicatorKey(indicator string) string {
	w.requirePrefix()
	return fmt.Sprintf("%s:samples:%s", w.prefix, encode.EncodeRedisCatalogName(indicator))
}
