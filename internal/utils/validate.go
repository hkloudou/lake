package utils

import (
	"fmt"
	"regexp"
)

// Length caps, in bytes, checked before the charset regexes (oversize input
// is rejected without paying a regex scan, and without echoing itself into
// the error). Unbounded names would pass validation here and then fail far
// away with a confusing storage- or Redis-level error — or quietly bloat the
// index.
const (
	// MaxCatalogLen bounds catalog names (and sample indicators, which share
	// the rules). The binding constraint is the object path: a mixed-case /
	// non-ASCII catalog is base32-encoded into ONE path component
	// (objkey.encode), and the file backend maps components to directory
	// names capped at 255 bytes on Linux — base32(128) = 208, safely under.
	// Every other consumer (Redis keys, OSS/S3 keys ≤ 1023 bytes) has more
	// headroom than that.
	MaxCatalogLen = 128
	// MaxFieldPathLen bounds the JSON field path. It is recorded verbatim in
	// every delta member (Redis memory, paid per write) and replayed through
	// the merge engine on every read.
	MaxFieldPathLen = 512
	// MaxStoragePartLen bounds provider / bucket names embedded in object
	// URIs. Real object stores cap bucket names lower still (OSS / S3: 63
	// chars), so a longer value can only be a mistake.
	MaxStoragePartLen = 63
)

// fieldPathRegex: JSON field path used in delta members.
//
//   - starts with "/", does not end with "/"
//   - each segment matches [a-zA-Z_$][a-zA-Z0-9_$.]*
//   - "|" forbidden — it is the delta-member delimiter
var fieldPathRegex = regexp.MustCompile(`^/([a-zA-Z_$][a-zA-Z0-9_$.]*(/[a-zA-Z_$][a-zA-Z0-9_$.]*)*)?$`)

func ValidateFieldPath(path string) error {
	if len(path) > MaxFieldPathLen {
		return fmt.Errorf("invalid field path: %d bytes exceeds the %d-byte limit", len(path), MaxFieldPathLen)
	}
	if !fieldPathRegex.MatchString(path) {
		return fmt.Errorf("invalid field path %q: start with /, no trailing /, segments must follow JS variable naming", path)
	}
	return nil
}

// catalogRegex: catalog name. Forbids ":" / "|" / "(" / ")" (Redis &
// delta delimiters / OSS encoding markers), leading "-" or "." (shell
// flag / hidden-file conventions), "/" at start/end/doubled, and
// non-ASCII (Unicode NFC/NFD divergence breaks Redis equality).
var catalogRegex = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.\-]*(/[a-zA-Z0-9_][a-zA-Z0-9_.\-]*)*$`)

func ValidateCatalog(catalog string) error {
	if len(catalog) > MaxCatalogLen {
		return fmt.Errorf("invalid catalog: %d bytes exceeds the %d-byte limit", len(catalog), MaxCatalogLen)
	}
	if !catalogRegex.MatchString(catalog) {
		return fmt.Errorf(`invalid catalog %q: each segment ASCII [a-zA-Z0-9_][a-zA-Z0-9_.\-]*; segments joined by single "/"; no leading/trailing/doubled "/"; ":" "|" "(" ")" forbidden`, catalog)
	}
	return nil
}

// storagePartRegex: storage provider / bucket name, as embedded in object
// URIs ("provider://bucket/path"). ParseURI splits a URI on the FIRST "://"
// and the FIRST "/" after it, so neither part may contain "/" or ":" —
// otherwise BuildURI/ParseURI disagree and the recorded locator resolves to
// a different object than the one written. Conservative ASCII charset; no
// leading "." or "-" (hidden-file / flag conventions, and "-internal"-style
// endpoint suffixing must not be forgeable via the name).
var storagePartRegex = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._\-]*$`)

func ValidateStorageProvider(provider string) error {
	return validateStoragePart("provider", provider)
}

func ValidateStorageBucket(bucket string) error {
	return validateStoragePart("bucket", bucket)
}

// validateStoragePart is the single definition of the URI-part format; the
// two exported wrappers exist only to name the offending part in the error.
func validateStoragePart(kind, s string) error {
	if len(s) > MaxStoragePartLen {
		return fmt.Errorf("invalid storage %s: %d bytes exceeds the %d-byte limit", kind, len(s), MaxStoragePartLen)
	}
	if !storagePartRegex.MatchString(s) {
		return fmt.Errorf(`invalid storage %s %q: ASCII [a-zA-Z0-9][a-zA-Z0-9._\-]* required ("/" ":" "|" forbidden)`, kind, s)
	}
	return nil
}
