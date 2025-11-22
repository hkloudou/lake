package encode

import (
	"crypto/md5"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

// EncodeOssCatalogName encodes catalog name for OSS paths
// Returns the encoded name with prefix for type identification
func EncodeOssCatalogName(catalog string) string {
	// Check if all lowercase safe
	if IsOssLowerSafe(catalog) {
		// Prefix: ( for lowercase
		return "(" + catalog
	}

	// Check if all uppercase safe
	if IsOssUpperSafe(catalog) {
		// Prefix: ) for uppercase
		return ")" + catalog
	}

	// Mixed case or unsafe characters: use base32 lowercase
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte(catalog))
	return strings.ToLower(encoded)
}

// EncodeRedisCatalogName encodes catalog name for Redis keys
// Uses base64 URL encoding (safe for Redis keys)
func EncodeRedisCatalogName(catalog string) string {
	// For Redis, if catalog is safe, use as-is; otherwise base64
	if IsRedisSafe(catalog) {
		return catalog
	}
	return base64.URLEncoding.EncodeToString([]byte(catalog))
}

// EncodeOssCatalogPath generates OSS path with MD5 sharding
// Format: md5(catalog)[0:shardSize]/EncodeOssCatalogName(catalog)
func EncodeOssCatalogPath(catalog string, shardSize int) string {
	hash := md5.Sum([]byte(catalog))
	md5Prefix := hex.EncodeToString(hash[:])[0:shardSize]
	catalogEncoded := EncodeOssCatalogName(catalog)
	return md5Prefix + "/" + catalogEncoded
}

// IsOssLowerSafe checks if catalog contains only lowercase safe characters
// Allows: a-z, 0-9, -, _, /, .
func IsOssLowerSafe(catalog string) bool {
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

// IsOssUpperSafe checks if catalog contains only uppercase safe characters
// Allows: A-Z, 0-9, -, _, /, .
func IsOssUpperSafe(catalog string) bool {
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

// IsRedisSafe checks if catalog is safe for Redis keys
// Allows: a-z, A-Z, 0-9, -, _, :, .
// Redis keys can handle more characters than OSS paths
func IsRedisSafe(catalog string) bool {
	if len(catalog) == 0 {
		return false
	}
	for _, r := range catalog {
		if !((r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == ':' || r == '.') {
			return false
		}
	}
	return true
}
