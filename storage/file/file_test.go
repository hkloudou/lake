package file

import (
	"context"
	"os"
	"path/filepath"
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
