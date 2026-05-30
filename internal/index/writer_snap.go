package index

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Writer writes Redis index entries (delta zset, snap hash, seqid).
type Writer struct {
	rdb *redis.Client
	indexIO
}

// NewWriter returns a Writer; SetPrefix must be called before use.
func NewWriter(rdb *redis.Client) *Writer {
	return &Writer{rdb: rdb}
}

// AddSnap upserts the catalog's snap entry in "<prefix>:s" as [tsSeq, uri].
// HSet overwrites any prior entry; the previous snap object is left orphan in
// storage (V3 contract).
func (w *Writer) AddSnap(ctx context.Context, catalog string, stopTsSeq TimeSeqID, uri string) error {
	val, err := EncodeSnapValue(stopTsSeq, uri)
	if err != nil {
		return err
	}
	return w.rdb.HSet(ctx, w.MakeSnapsHashKey(), catalog, val).Err()
}
