// Package file is a local-filesystem storage backend for dev / single-node
// use. A bucket maps to a sub-directory under the base path. It writes via a
// temp file + atomic rename and does not support presign.
package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hkloudou/lake/v3/storage"
)

// FS roots all buckets under BasePath.
type FS struct{ base string }

// New roots the backend at basePath (created if missing).
func New(basePath string) (*FS, error) {
	abs, err := filepath.Abs(basePath)
	if err != nil {
		return nil, fmt.Errorf("file: resolve base: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("file: mkdir %s: %w", abs, err)
	}
	return &FS{base: abs}, nil
}

// Bucket returns a bucket-scoped Storage (bucket = sub-directory).
func (f *FS) Bucket(name string) storage.Storage { return &view{root: filepath.Join(f.base, name)} }

type view struct{ root string }

func (v *view) full(path string) string { return filepath.Join(v.root, filepath.FromSlash(path)) }

func (v *view) Get(_ context.Context, _ /*catalog*/, path string) ([]byte, error) {
	data, err := os.ReadFile(v.full(path))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file: not found: %s", path)
		}
		return nil, fmt.Errorf("file: read %s: %w", path, err)
	}
	return data, nil
}

func (v *view) Put(_ context.Context, _ /*catalog*/, path string, data []byte) error {
	full := v.full(path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return fmt.Errorf("file: mkdir: %w", err)
	}
	tmp := full + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("file: write tmp: %w", err)
	}
	if err := os.Rename(tmp, full); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("file: rename: %w", err)
	}
	return nil
}
