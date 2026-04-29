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

// AddSnap upserts the catalog's snap entry in "<prefix>:snaps".
// HSet overwrites any prior entry; the previous OSS snap object is
// left orphan in storage (V3 contract).
func (w *Writer) AddSnap(ctx context.Context, catalog string, stopTsSeq TimeSeqID) error {
	return w.rdb.HSet(ctx, w.MakeSnapsHashKey(), catalog, EncodeSnapValue(stopTsSeq)).Err()
}
