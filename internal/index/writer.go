package index

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Writer handles writing to Redis ZADD index
type Writer struct {
	rdb *redis.Client
}

// NewWriter creates a new index writer
func NewWriter(rdb *redis.Client) *Writer {
	return &Writer{rdb: rdb}
}

// Add adds an entry to the catalog index
// Uses ZADD with timestamp as score and "field:uuid" as member
func (w *Writer) Add(ctx context.Context, catalog, field, uuid string, timestamp int64) error {
	key := makeCatalogKey(catalog)
	member := EncodeMember(field, uuid)

	return w.rdb.ZAdd(ctx, key, redis.Z{
		Score:  float64(timestamp),
		Member: member,
	}).Err()
}

// AddSnap adds a snapshot entry to the catalog snapshot index
func (w *Writer) AddSnap(ctx context.Context, catalog, snapUUID string, timestamp int64) error {
	key := makeSnapKey(catalog)
	member := EncodeSnapMember(snapUUID)

	return w.rdb.ZAdd(ctx, key, redis.Z{
		Score:  float64(timestamp),
		Member: member,
	}).Err()
}

// BatchAdd adds multiple entries in a pipeline for better performance
func (w *Writer) BatchAdd(ctx context.Context, catalog string, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	pipe := w.rdb.Pipeline()
	key := makeCatalogKey(catalog)

	for _, e := range entries {
		member := EncodeMember(e.Field, e.UUID)
		pipe.ZAdd(ctx, key, redis.Z{
			Score:  float64(e.Timestamp),
			Member: member,
		})
	}

	_, err := pipe.Exec(ctx)
	return err
}

// Entry represents an index entry
type Entry struct {
	Field     string
	UUID      string
	Timestamp int64
}

func makeCatalogKey(catalog string) string {
	return fmt.Sprintf("catalog:%s", catalog)
}

func makeSnapKey(catalog string) string {
	return fmt.Sprintf("catalog:%s:snap", catalog)
}
