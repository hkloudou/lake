// Package objkey renders Lake's canonical object paths and storage URIs.
// Both are part of the Lake data spec: a delta's URI (provider://bucket/path)
// is a complete, portable object locator that any S3/OSS tool can fetch.
package objkey

import (
	"crypto/md5"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"strings"
)

// DeltaPath: "{md5(catalog)[0:4]}/{enc(catalog)}/{uuid}.dat".
func DeltaPath(catalog, uuid string) string {
	return prefix(catalog) + "/" + uuid + ".dat"
}

// SnapPath: "{md5(catalog)[0:4]}/{enc(catalog)}/{stopTsSeq}.snap".
func SnapPath(catalog, stopTsSeq string) string {
	return prefix(catalog) + "/" + stopTsSeq + ".snap"
}

// prefix shards by md5[0:4] (65,536 hot-spot-free buckets-within-a-bucket)
// then keeps a human-readable per-catalog folder for lifecycle / billing.
func prefix(catalog string) string {
	h := md5.Sum([]byte(catalog))
	return hex.EncodeToString(h[:])[0:4] + "/" + encode(catalog)
}

// encode renders a catalog for path safety. Catalog validation already forbids
// the marker chars "(" ")" ":" "|", so the three forms never collide:
//
//   - "(name"   pure lowercase (a-z 0-9 - _ / .)
//   - ")NAME"   pure uppercase
//   - <base32>  mixed-case / non-ascii (lowercased base32, no padding)
func encode(catalog string) string {
	if caseSafe(catalog, 'a', 'z') {
		return "(" + catalog
	}
	if caseSafe(catalog, 'A', 'Z') {
		return ")" + catalog
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte(catalog)))
}

func caseSafe(s string, lo, hi rune) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= lo && r <= hi:
		case r >= '0' && r <= '9':
		case r == '-', r == '_', r == '/', r == '.':
		default:
			return false
		}
	}
	return true
}

// BuildURI assembles "provider://bucket/path".
func BuildURI(provider, bucket, path string) string {
	return provider + "://" + bucket + "/" + path
}

// ParseURI splits "provider://bucket/path" into its three parts.
func ParseURI(uri string) (provider, bucket, path string, err error) {
	scheme, rest, ok := strings.Cut(uri, "://")
	if !ok || scheme == "" {
		return "", "", "", fmt.Errorf("invalid storage URI %q (want provider://bucket/path)", uri)
	}
	bkt, key, ok := strings.Cut(rest, "/")
	if !ok || bkt == "" || key == "" {
		return "", "", "", fmt.Errorf("invalid storage URI %q (want provider://bucket/path)", uri)
	}
	return scheme, bkt, key, nil
}
