package cached

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"
)

// gzipLevel trades a few percent of ratio for several-fold less CPU vs
// BestCompression — cache writes happen on the read path (write-through
// snapshot warming), so compression latency is user-visible. JSON at the
// default level already compresses within a few percent of level 9.
const gzipLevel = gzip.DefaultCompression

// Writers carry ~KBs of deflate state each; pooling them removes the
// dominant allocation of every cache write.
var gzipWriterPool = sync.Pool{
	New: func() any {
		w, _ := gzip.NewWriterLevel(io.Discard, gzipLevel)
		return w
	},
}

var gzipReaderPool = sync.Pool{New: func() any { return new(gzip.Reader) }}

// gzipCompress compresses data. Snap-cache values are JSON documents that
// compress well, so storing them gzipped meaningfully cuts cache-Redis
// memory. This is a pure space optimization — NOT encryption; the cache
// holds only data that is also derivable from the authoritative store.
func gzipCompress(data []byte) ([]byte, error) {
	w := gzipWriterPool.Get().(*gzip.Writer)
	defer gzipWriterPool.Put(w)

	var buf bytes.Buffer
	buf.Grow(len(data)/3 + 64) // JSON typically compresses 3x or better
	w.Reset(&buf)
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
// The returned slice is freshly allocated — callers own it.
func gunzip(data []byte) ([]byte, error) {
	r := gzipReaderPool.Get().(*gzip.Reader)
	defer gzipReaderPool.Put(r)
	if err := r.Reset(bytes.NewReader(data)); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(len(data) * 4) // pre-size near the typical JSON ratio
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
