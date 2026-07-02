package file

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestFile_RoundTrip(t *testing.T) {
	fs, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := fs.Bucket("data")
	ctx := context.Background()
	if err := b.Put(ctx, "users", "ab/cd/x.dat", []byte("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := b.Get(ctx, "users", "ab/cd/x.dat")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("Get = %q, want hello", got)
	}
}

// TestFile_ConcurrentPutSamePath pins the staging contract: every writer
// stages through its OWN temp file, so racing Puts of one path never
// interleave bytes — the final content is exactly one writer's payload, and
// no *.tmp* staging file survives.
func TestFile_ConcurrentPutSamePath(t *testing.T) {
	fs, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := fs.Bucket("data")
	ctx := context.Background()

	payload := func(i int) []byte {
		return append([]byte{byte('a' + i)}, make([]byte, 64*1024)...)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			if err := b.Put(ctx, "users", "x.snap", payload(i)); err != nil {
				t.Errorf("Put: %v", err)
			}
		})
	}
	wg.Wait()

	got, err := b.Get(ctx, "users", "x.snap")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var whole bool
	for i := 0; i < 8; i++ {
		if string(got) == string(payload(i)) {
			whole = true
			break
		}
	}
	if !whole {
		t.Fatalf("content is not any single writer's payload (len=%d, first byte %q) — writes interleaved", len(got), got[0])
	}

	matches, err := filepath.Glob(filepath.Join(fs.base, "data", "*.tmp*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("staging files left behind: %v", matches)
	}
}

// TestFile_RejectsTraversal pins the root-confinement guard: a path that escapes
// the bucket root via ".." must be rejected on both Get and Put, and must never
// touch a file outside the root.
func TestFile_RejectsTraversal(t *testing.T) {
	base := t.TempDir()
	canary := filepath.Join(base, "secret.txt") // one level above the bucket root
	if err := os.WriteFile(canary, []byte("top secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	fs, err := New(filepath.Join(base, "store"))
	if err != nil {
		t.Fatal(err)
	}
	b := fs.Bucket("data") // root = <base>/store/data
	ctx := context.Background()

	for _, p := range []string{"../../secret.txt", "../secret.txt", "a/../../../secret.txt"} {
		if _, err := b.Get(ctx, "users", p); err == nil {
			t.Errorf("Get(%q) should be rejected", p)
		}
		if err := b.Put(ctx, "users", p, []byte("pwned")); err == nil {
			t.Errorf("Put(%q) should be rejected", p)
		}
	}
	if data, _ := os.ReadFile(canary); string(data) != "top secret" {
		t.Fatalf("canary was modified through traversal: %q", data)
	}
}

func TestFile_RejectsEscapingBucket(t *testing.T) {
	base := t.TempDir()
	canary := filepath.Join(base, "secret.txt")
	if err := os.WriteFile(canary, []byte("top secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	fs, err := New(filepath.Join(base, "store"))
	if err != nil {
		t.Fatal(err)
	}
	b := fs.Bucket("..")
	ctx := context.Background()

	if _, err := b.Get(ctx, "users", "secret.txt"); err == nil {
		t.Fatal("Get with escaping bucket should be rejected")
	}
	if err := b.Put(ctx, "users", "secret.txt", []byte("pwned")); err == nil {
		t.Fatal("Put with escaping bucket should be rejected")
	}
	if data, _ := os.ReadFile(canary); string(data) != "top secret" {
		t.Fatalf("canary was modified through escaping bucket: %q", data)
	}
}
