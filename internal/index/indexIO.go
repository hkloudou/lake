package index

import (
	"github.com/hkloudou/lake/v3/internal/encode"
)

// indexIO holds the deployment-level Redis key prefix and renders the
// canonical Redis keys used by Reader / Writer. All keys live under
// "<prefix>:..." so different deployments sharing one Redis stay
// isolated by Name. The fixed keys are rendered once in SetPrefix — they
// sit on every hot path (List, BatchList per catalog, every sample probe),
// so they must not be re-formatted per call.
type indexIO struct {
	prefix   string
	snapsKey string // "<prefix>:s"
	mrgKey   string // "<prefix>:mrg"
}

func (w *indexIO) SetPrefix(p string) {
	w.prefix = p
	w.snapsKey = p + ":s"
	w.mrgKey = p + ":mrg"
}
func (w *indexIO) Prefix() string { return w.prefix }

func (w *indexIO) requirePrefix() {
	if w.prefix == "" {
		panic("index: prefix not set; call SetPrefix before use")
	}
}

// MakeDeltaZsetKey: per-catalog delta ZSet "<prefix>:d:<catalog>".
func (w *indexIO) MakeDeltaZsetKey(catalog string) string {
	w.requirePrefix()
	return w.prefix + ":d:" + encode.EncodeRedisCatalogName(catalog)
}

// MakeSnapsHashKey: deployment-wide snap Hash "<prefix>:s", with catalog
// as field. One HMGet/HGETALL surfaces every snap at once.
func (w *indexIO) MakeSnapsHashKey() string {
	w.requirePrefix()
	return w.snapsKey
}

// MakeSampleIndicatorKey: per-indicator sample Hash
// "<prefix>:m:<indicator>", with catalog as field. "m" reads as "memo" —
// a sample is the memoised output of a derived computation.
func (w *indexIO) MakeSampleIndicatorKey(indicator string) string {
	w.requirePrefix()
	return w.prefix + ":m:" + encode.EncodeRedisCatalogName(indicator)
}

// MakeSampleRemoveGenKey: deployment-wide catalog-level sample removal
// generation Hash "<prefix>:mrg", with catalog as field. Unlike the
// per-indicator epoch (which lives inside each memo hash and so cannot
// guard an indicator that has never cached anything), this key is created
// by the first HINCRBY — it exists independently of any memo hash, so a
// RemoveDelta can void in-flight sample writes for indicators Lake has
// never seen. Cannot collide with "<prefix>:m:<indicator>" (that form
// always has a second ":").
func (w *indexIO) MakeSampleRemoveGenKey() string {
	w.requirePrefix()
	return w.mrgKey
}

// MakeSeqAllocKey: per-catalog tsSeq allocator "<prefix>:seq:<catalog>",
// holding the last issued "ts_seq" pair (see notifyLua, writer_atomic.go).
func (w *indexIO) MakeSeqAllocKey(catalog string) string {
	w.requirePrefix()
	return w.prefix + ":seq:" + encode.EncodeRedisCatalogName(catalog)
}
