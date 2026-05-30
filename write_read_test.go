package lake

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/gjson"
)

// presignBucket wraps a mem bucket with a dummy presigner so WriteBegin works
// against the in-memory backend in tests (the "upload" is a direct Put).
type presignBucket struct{ storage.Storage }

func (presignBucket) PresignPut(context.Context, string, string, storage.PresignOptions) (storage.PresignedUpload, error) {
	return storage.PresignedUpload{URL: "mem://upload", Method: "PUT"}, nil
}

// TestWriteReadRoundTrip_Redis exercises the full new-model flow against a real
// Redis: WriteBegin (presign) → direct upload → WriteNotify (URI in the delta)
// → List → Read (resolve URI → fetch → merge). Skips when Redis is unreachable.
func TestWriteReadRoundTrip_Redis(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 13, DialTimeout: 200 * time.Millisecond})
	pingCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		t.Skipf("redis not reachable, skipping integration test: %v", err)
	}
	if err := rdb.FlushDB(pingCtx).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}

	store := mem.New()
	resolve := func(provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New("test", rdb, resolve, WithSnapTarget("mem", "snaps"))

	ctx := context.Background()
	write := func(path string, mt MergeType, body string) {
		t.Helper()
		h, err := c.WriteBegin(ctx, WriteBeginRequest{
			Catalog: "users", Path: path, MergeType: mt, Provider: "mem", Bucket: "data",
		})
		if err != nil {
			t.Fatalf("WriteBegin(%s): %v", path, err)
		}
		// Simulate the client's direct upload to the presigned location.
		if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(body)); err != nil {
			t.Fatalf("upload: %v", err)
		}
		if err := c.WriteNotify(ctx, h); err != nil {
			t.Fatalf("WriteNotify(%s): %v", path, err)
		}
		if !strings.HasPrefix(h.URI, "mem://data/") {
			t.Fatalf("handle URI = %q, want mem://data/ prefix", h.URI)
		}
	}

	write("/", MergeTypeReplace, `{"name":"Alice","age":30}`)
	write("/profile", MergeTypeRFC7396, `{"city":"NYC","age":31}`)

	list := c.List(ctx, "users")
	if list.Err != nil {
		t.Fatalf("List: %v", list.Err)
	}
	got, err := ReadString(ctx, list)
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	r := gjson.Parse(got)
	if r.Get("name").String() != "Alice" {
		t.Errorf("name = %q, want Alice (doc: %s)", r.Get("name").String(), got)
	}
	if r.Get("age").Int() != 30 {
		t.Errorf("age = %d, want 30 (doc: %s)", r.Get("age").Int(), got)
	}
	if r.Get("profile.city").String() != "NYC" || r.Get("profile.age").Int() != 31 {
		t.Errorf("profile = %s, want {city:NYC,age:31}", r.Get("profile").Raw)
	}
}
