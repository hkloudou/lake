package index

import (
	"encoding/base64"
	"fmt"
)

// makeCatalogKey generates the Redis key for catalog data index
// Format: Storage:Name:delta:$catalog (e.g., "oss:my-lake:delta:users")
// func (r *Reader) makeCatalogKey(catalog string) string {
// 	if r.prefix == "" {
// 		panic("prefix is not set")
// 	}
// 	return fmt.Sprintf("%s:delta:%s", r.prefix, catalog)
// }

// // makeSnapKey generates the Redis key for catalog snapshot index
// // Format: Storage:Name:snap:$catalog (e.g., "oss:my-lake:snap:users")
//
//	func (r *Reader) makeSnapKey(catalog string) string {
//		if r.prefix == "" {
//			panic("prefix is not set")
//		}
//		return fmt.Sprintf("%s:snap:%s", r.prefix, catalog)
//	}
type indexKey struct {
	prefix string
}

func (w *indexKey) makeCatalogKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:delta:%s", w.prefix, base64.URLEncoding.EncodeToString([]byte(catalog)))
}

// makeSnapKey generates the Redis key for catalog snapshot index
// Kept in sync with Reader.makeSnapKey in keys.go
func (w *indexKey) makeSnapKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:snap:%s", w.prefix, base64.URLEncoding.EncodeToString([]byte(catalog)))
}

// SetPrefix sets the key prefix (e.g., "oss:my-lake")
func (w *indexKey) SetPrefix(prefix string) {
	w.prefix = prefix
}
