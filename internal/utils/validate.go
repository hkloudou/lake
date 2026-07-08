package utils

import (
	"fmt"
	"regexp"
)

// Length caps, in bytes, enforced ONLY where a name is being created
// (ValidateNewCatalog / ValidateNewFieldPath — i.e. WriteBegin, WriteNotify,
// NewSampler): unbounded new names would pass the charset checks and then
// fail far away with a confusing storage- or Redis-level error — or quietly
// bloat the index. Read and ops paths (List, RemoveDelta, Compact, member
// decoding) deliberately do NOT apply them: data persisted under a longer
// name when the cap was laxer must stay readable and removable — tightening
// a cap must never strand existing data. The cap check runs before the
// charset regex (oversize input is rejected without paying a regex scan,
// and without echoing itself into the error).
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
	// URIs. This is Lake's own backend-agnostic sanity bound — both parts are
	// recorded in every delta's URI, and a bucket maps to one path component
	// on the file backend (255-byte limit) — NOT a cloud rule: real object
	// stores impose tighter limits of their own (OSS / S3 buckets: 63 chars)
	// and surface them from the backend itself, while file / mem / custom
	// resolvers may use longer logical names up to this bound.
	MaxStoragePartLen = 128
)

// fieldPathRegex: JSON field path used in delta members.
//
//   - starts with "/", does not end with "/"
//   - each segment matches [a-zA-Z_$][a-zA-Z0-9_$.]*
//   - "|" forbidden — it is the delta-member delimiter
var fieldPathRegex = regexp.MustCompile(`^/([a-zA-Z_$][a-zA-Z0-9_$.]*(/[a-zA-Z_$][a-zA-Z0-9_$.]*)*)?$`)

// ValidateFieldPath checks the charset/shape rules only — the read-side
// check, applied to paths already persisted in delta members.
func ValidateFieldPath(path string) error {
	if !fieldPathRegex.MatchString(path) {
		return fmt.Errorf("invalid field path %q: start with /, no trailing /, segments must follow JS variable naming", path)
	}
	return nil
}

// ValidateNewFieldPath additionally enforces MaxFieldPathLen — the write-side
// check for paths about to be recorded (WriteBegin / WriteNotify).
func ValidateNewFieldPath(path string) error {
	if len(path) > MaxFieldPathLen {
		return fmt.Errorf("invalid field path: %d bytes exceeds the %d-byte limit", len(path), MaxFieldPathLen)
	}
	return ValidateFieldPath(path)
}

// catalogRegex: catalog name. Forbids ":" / "|" / "(" / ")" (Redis &
// delta delimiters / OSS encoding markers), leading "-" or "." (shell
// flag / hidden-file conventions), "/" at start/end/doubled, and
// non-ASCII (Unicode NFC/NFD divergence breaks Redis equality).
var catalogRegex = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.\-]*(/[a-zA-Z0-9_][a-zA-Z0-9_.\-]*)*$`)

// ValidateCatalog checks the charset/shape rules only — the read/ops-side
// check (List, BatchList, RemoveDelta, Compact, InvalidateSamples), so a
// catalog written under a laxer cap stays reachable.
func ValidateCatalog(catalog string) error {
	if !catalogRegex.MatchString(catalog) {
		return fmt.Errorf(`invalid catalog %q: each segment ASCII [a-zA-Z0-9_][a-zA-Z0-9_.\-]*; segments joined by single "/"; no leading/trailing/doubled "/"; ":" "|" "(" ")" forbidden`, catalog)
	}
	return nil
}

// ValidateNewCatalog additionally enforces MaxCatalogLen — the write-side
// check for names about to mint new state (WriteBegin / WriteNotify deltas,
// NewSampler memo fields).
func ValidateNewCatalog(catalog string) error {
	if len(catalog) > MaxCatalogLen {
		return fmt.Errorf("invalid catalog: %d bytes exceeds the %d-byte limit", len(catalog), MaxCatalogLen)
	}
	return ValidateCatalog(catalog)
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
