package utils

import (
	"fmt"
	"regexp"
)

// Length caps, in bytes. A catalog is base32-encoded into ONE object-path
// component for mixed-case names (base32(128) = 208, under the 255-byte
// filesystem limit); the field path is recorded verbatim in every delta
// member; provider / bucket are embedded in every URI (real object stores
// impose tighter limits of their own and report them themselves).
const (
	MaxCatalogLen     = 128
	MaxFieldPathLen   = 512
	MaxStoragePartLen = 128
)

var (
	// Field path: starts with "/", no trailing "/", segments follow JS
	// variable naming. No "." inside a segment: "/" is the only separator,
	// so a path maps to a gjson path by replacing "/" with "." and nothing
	// ever needs escaping.
	fieldPathRegex = regexp.MustCompile(`^/([a-zA-Z_$][a-zA-Z0-9_$]*(/[a-zA-Z_$][a-zA-Z0-9_$]*)*)?$`)
	// Catalog (and sample indicator): ASCII segments joined by single "/".
	// ":" "|" "(" ")" are Redis / member delimiters and path-encoding markers.
	catalogRegex = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.\-]*(/[a-zA-Z0-9_][a-zA-Z0-9_.\-]*)*$`)
	// Provider / bucket: ParseURI splits on the first "://" and the first
	// "/", so neither part may contain "/" or ":".
	storagePartRegex = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._\-]*$`)
)

func ValidateFieldPath(path string) error {
	if len(path) > MaxFieldPathLen {
		return fmt.Errorf("invalid field path: %d bytes exceeds the %d-byte limit", len(path), MaxFieldPathLen)
	}
	if !fieldPathRegex.MatchString(path) {
		return fmt.Errorf("invalid field path %q: start with /, no trailing /, segments [a-zA-Z_$][a-zA-Z0-9_$]* (no dots)", path)
	}
	return nil
}

func ValidateCatalog(catalog string) error {
	if len(catalog) > MaxCatalogLen {
		return fmt.Errorf("invalid catalog: %d bytes exceeds the %d-byte limit", len(catalog), MaxCatalogLen)
	}
	if !catalogRegex.MatchString(catalog) {
		return fmt.Errorf(`invalid catalog %q: each segment ASCII [a-zA-Z0-9_][a-zA-Z0-9_.\-]*; segments joined by single "/"; ":" "|" "(" ")" forbidden`, catalog)
	}
	return nil
}

func ValidateStorageProvider(provider string) error { return validateStoragePart("provider", provider) }
func ValidateStorageBucket(bucket string) error     { return validateStoragePart("bucket", bucket) }

func validateStoragePart(kind, s string) error {
	if len(s) > MaxStoragePartLen {
		return fmt.Errorf("invalid storage %s: %d bytes exceeds the %d-byte limit", kind, len(s), MaxStoragePartLen)
	}
	if !storagePartRegex.MatchString(s) {
		return fmt.Errorf(`invalid storage %s %q: ASCII [a-zA-Z0-9][a-zA-Z0-9._\-]* required ("/" ":" "|" forbidden)`, kind, s)
	}
	return nil
}
