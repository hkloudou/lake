package cache

import (
	"bytes"
	"compress/gzip"
	"io"
)

// gzipCompress compresses data at best compression. Snap-cache values are
// JSON documents that compress well, so storing them gzipped meaningfully
// cuts cache-Redis memory. This is a pure space optimization — NOT
// encryption; the cache holds only data that is also derivable from the
// authoritative store.
func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// gunzip reverses gzipCompress. A decompression error means the stored
// value is foreign or in a legacy format; callers treat that as a cache
// miss and recompute, so a format change never surfaces corrupt bytes.
func gunzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
