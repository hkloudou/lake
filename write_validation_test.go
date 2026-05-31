package lake

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
	"github.com/redis/go-redis/v9"
)

func TestWriteNotify_RejectsInvalidMergeTypeBeforeRedis(t *testing.T) {
	c := newDeadClient(t)
	err := c.WriteNotify(context.Background(), &WriteHandle{
		Catalog:   "users",
		Path:      "/",
		MergeType: MergeTypeUnknown,
		URI:       "mem://data/object.dat",
	})
	if err == nil {
		t.Fatal("expected invalid mergeType error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid mergeType") {
		t.Fatalf("expected invalid mergeType error, got %v", err)
	}
}

func TestWriteNotify_RejectsMalformedURIBeforeRedis(t *testing.T) {
	c := newDeadClient(t)
	err := c.WriteNotify(context.Background(), &WriteHandle{
		Catalog:   "users",
		Path:      "/",
		MergeType: MergeTypeReplace,
		URI:       "oops",
	})
	if err == nil {
		t.Fatal("expected invalid storage URI error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid storage URI") {
		t.Fatalf("expected invalid storage URI error, got %v", err)
	}
}

func TestWriteBegin_ZeroTTLUsesDefaultTTL(t *testing.T) {
	store := mem.New()
	resolve := func(_ storage.Kind, _, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = rdb.Close() })
	c := New("ttltest", rdb, resolve)

	h, err := c.WriteBegin(context.Background(), WriteBeginRequest{
		Catalog:   "users",
		Path:      "/",
		MergeType: MergeTypeReplace,
		Provider:  "mem",
		Bucket:    "data",
	}, WithUploadTTL(0))
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}

	ttl := h.ExpiresAt - time.Now().Unix()
	if ttl < 14*60 || ttl > 16*60 {
		t.Fatalf("ExpiresAt delta = %ds, want about 15m", ttl)
	}
}
