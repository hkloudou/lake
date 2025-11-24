package encode

import (
	"encoding/base64"
	"strings"
)

// EncodeRedisCatalogName encodes catalog name for Redis keys
// Uses base64 URL encoding without padding (safe for Redis keys)
func EncodeRedisCatalogName(catalog string) string {
	if len(catalog) == 0 {
		return "" // Return empty string for empty catalog
	}

	// For Redis, if catalog is safe, use as-is with prefix
	if IsRedisSafe(catalog) {
		return "(" + catalog
	}
	// Use base64 URL encoding without padding
	return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString([]byte(catalog))
}

func DecodeRedisCatalogName(catalog string) (string, error) {
	// For Redis, if catalog is safe, use as-is with prefix
	// if IsRedisSafe(catalog) {
	// 	return "(" + catalog
	// }
	if len(catalog) == 0 {
		return "", nil
	}
	if strings.HasPrefix(catalog, "(") {
		return catalog[1:], nil
	}
	// Use base64 URL encoding without padding
	decoded, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(catalog)
	if err != nil {
		return "", err
	}
	// fmt.Println("catalog", catalog, "decoded", string(decoded))
	return string(decoded), nil
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
			r == '-' || r == '_' || r == '/' || r == '.') {
			return false
		}
	}
	return true
}
