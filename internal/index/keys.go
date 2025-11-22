package index

import "fmt"

// makeCatalogKey generates the Redis key for catalog data index
// Format: Storage:Name:delta:$catalog (e.g., "oss:my-lake:delta:users")
func (r *Reader) makeCatalogKey(catalog string) string {
	if r.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:delta:%s", r.prefix, catalog)
}

// makeSnapKey generates the Redis key for catalog snapshot index
// Format: Storage:Name:snap:$catalog (e.g., "oss:my-lake:snap:users")
func (r *Reader) makeSnapKey(catalog string) string {
	if r.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:snap:%s", r.prefix, catalog)
}
