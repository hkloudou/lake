package lake

import (
	"context"
	"strings"
	"testing"

	"github.com/hkloudou/lake/v3/internal/objkey"
	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
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
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve, WithSnapTarget("mem", "snaps"))

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

	// ReadString triggered an async snapshot save (WithSnapTarget). Wait for it to
	// be indexed, both to exercise that path and so its <prefix>:s write lands
	// before cleanup (a fire-and-forget goroutine otherwise writes after Cleanup).
	if !waitFor(func() bool { s, _ := c.reader.GetLatestSnap(ctx, "users"); return s != nil }) {
		t.Fatal("snapshot was not indexed within timeout")
	}
}

// TestReadBytesMutationDoesNotCorruptSnapshot pins the isolation between the
// bytes ReadBytes hands the caller and the bytes the async snapshot save
// persists: the caller owns its slice and may mutate it immediately, and the
// snapshot must still capture the original merged document. (A shared slice
// here once meant a caller mutation could persist a corrupt snapshot, which
// then poisoned every later read of the catalog.)
func TestReadBytesMutationDoesNotCorruptSnapshot(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve, WithSnapTarget("mem", "snaps"))

	ctx := context.Background()
	h, err := c.WriteBegin(ctx, WriteBeginRequest{
		Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
	})
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	const doc = `{"name":"Alice"}`
	if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(doc)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := c.WriteNotify(ctx, h); err != nil {
		t.Fatalf("WriteNotify: %v", err)
	}

	list := c.List(ctx, "users")
	if list.Err != nil {
		t.Fatalf("List: %v", list.Err)
	}
	got, err := ReadBytes(ctx, list)
	if err != nil {
		t.Fatalf("ReadBytes: %v", err)
	}
	// Caller mutates its result right away, while the snapshot save may still
	// be running in the background.
	for i := range got {
		got[i] = 'X'
	}

	var snap *SnapInfo
	if !waitFor(func() bool { snap, _ = c.reader.GetLatestSnap(ctx, "users"); return snap != nil }) {
		t.Fatal("snapshot was not indexed within timeout")
	}
	_, _, path, err := objkey.ParseURI(snap.URI)
	if err != nil {
		t.Fatalf("parse snap URI %q: %v", snap.URI, err)
	}
	data, err := store.Bucket("snaps").Get(ctx, "users", path)
	if err != nil {
		t.Fatalf("fetch snap object: %v", err)
	}
	if string(data) != doc {
		t.Fatalf("snapshot content corrupted by caller mutation: got %q, want %q", data, doc)
	}
}
