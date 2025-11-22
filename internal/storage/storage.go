package storage

import (
	"context"
	"crypto/md5"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/hkloudou/lake/v2/internal/index"
)

// Storage is the interface for object storage (OSS/S3/Local)
type Storage interface {
	// Put stores data with the given key
	Put(ctx context.Context, key string, data []byte) error

	// Get retrieves data by key
	Get(ctx context.Context, key string) ([]byte, error)

	// Delete removes data by key
	Delete(ctx context.Context, key string) error

	// Exists checks if key exists
	Exists(ctx context.Context, key string) (bool, error)

	// List lists all keys with the given prefix
	List(ctx context.Context, prefix string) ([]string, error)

	RedisPrefix() string
}

// StreamStorage extends Storage with streaming support
// type StreamStorage interface {
// 	Storage

// 	// PutStream stores data from a reader
// 	PutStream(ctx context.Context, key string, reader io.Reader, size int64) error

// 	// GetStream retrieves data as a reader
// 	GetStream(ctx context.Context, key string) (io.ReadCloser, error)
// }

// MakeKey generates storage key for catalog and file identifier
// For data files: catalog/{ts}_{seqid}_{mergeTypeInt}.json
// For snap files: catalog/{uuid}.json (legacy format)
// func MakeKey(catalog, identifier string) string {
// 	return catalog + "/" + identifier + ".json"
// }

// encodeCatalogPath encodes catalog name following OSS best practices
// Uses MD5 for sharding + optimized encoding for identification
// shardSize: number of MD5 prefix chars for sharding (typically 4)
// Format: md5(catalog)[0:shardSize]/encode(catalog)
//
// Encoding Strategy (optimized for length):
//  1. If catalog contains only safe chars (a-z, A-Z, 0-9, -, _): use as-is
//  2. Otherwise: use base32 lowercase (20% shorter than hex, OSS-safe)
//
// Examples (shardSize=4):
//
//	"users"    -> MD5="9bc6..." encode="users"        -> "9bc6/users"
//	"Users"    -> MD5="f9aa..." encode="Users"        -> "f9aa/Users"
//	"短中文"    -> MD5="xxxx..." encode="base32lower"  -> "xxxx/46p23zfy..."
//
// Benefits:
//   - MD5 prefix: uniform distribution (65,536 dirs)
//   - Smart encoding: shortest safe representation
//   - OSS-safe: all lowercase (base32) or mixed-case safe chars
//   - No collisions: guaranteed unique per catalog
func encodeCatalogPath(catalog string, shardSize int) string {
	// MD5 hash for uniform shard distribution
	hash := md5.Sum([]byte(catalog))
	return hex.EncodeToString(hash[:])[0:shardSize] + "/" + encodeCatalogName(catalog)
}

// encodeCatalogName encodes catalog name with optimal compression
// Returns the shortest safe representation with prefix for type identification
func encodeCatalogName(catalog string) string {
	// Check if all lowercase safe
	if isLowerSafe(catalog) {
		// Prefix: _ (underscore) for lowercase
		// Example: "users" -> "_users"
		return "(" + catalog
	}

	// Check if all uppercase safe
	if isUpperSafe(catalog) {
		// Prefix: ^ (caret) for uppercase
		// Example: "USERS" -> "^USERS"
		return ")" + catalog
	}

	// Mixed case or unsafe characters: use base32 lowercase
	// No prefix for base32 (starts with lowercase letter or digit)
	// Base32 is ~20% shorter than hex and OSS case-insensitive safe
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte(catalog))
	return strings.ToLower(encoded)
}

// isLowerSafe checks if catalog contains only lowercase safe characters
// Allows: a-z, 0-9, -, _, /, .
func isLowerSafe(catalog string) bool {
	if len(catalog) == 0 {
		return false
	}
	for _, r := range catalog {
		if !((r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '/' || r == '.') {
			return false
		}
	}
	return true
}

// isUpperSafe checks if catalog contains only uppercase safe characters
// Allows: A-Z, 0-9, -, _, /, .
func isUpperSafe(catalog string) bool {
	if len(catalog) == 0 {
		return false
	}
	for _, r := range catalog {
		if !((r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '/' || r == '.') {
			return false
		}
	}
	return true
}

// MakeDeltaKey generates storage key for data files with MD5-sharded path
// Format: {md5[0:4]}/{hex(catalog)}/delta/{ts}_{seqid}_{mergeTypeInt}.json
// Example: f9aa/5573657273/delta/1700000000_123_1.json (for catalog "Users")
func MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string {
	shardedPath := encodeCatalogPath(catalog, 4) // Default: 4-char MD5 prefix (65,536 dirs)
	return fmt.Sprintf("%s/delta/%s_%d.json", shardedPath, tsSeqID.String(), mergeType)
}

// MakeSnapKey generates storage key for snapshot files with MD5-sharded path
// Format: {md5[0:4]}/{hex(catalog)}/snap/{startTsSeq}~{stopTsSeq}.snap
// Example: f9aa/5573657273/snap/1700000000_1~1700000100_500.snap (for catalog "Users")
func MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string {
	shardedPath := encodeCatalogPath(catalog, 4) // Default: 4-char MD5 prefix (65,536 dirs)
	return fmt.Sprintf("%s/snap/%s~%s.snap", shardedPath, startTsSeq.String(), stopTsSeq.String())
}
