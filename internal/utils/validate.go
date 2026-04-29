package utils

import (
	"fmt"
	"regexp"
)

// fieldPathRegex validates a JSON field path used in delta members.
//   - Starts with /
//   - Does not end with /
//   - Each segment must start with letter, _, or $ (digits not allowed as first char)
//   - CRITICAL: '|' (pipe) is not allowed — it is the delimiter inside the
//     delta member encoding "delta|{mergeType}|{fieldPath}|{tsSeq}".
var fieldPathRegex = regexp.MustCompile(`^/([a-zA-Z_$][a-zA-Z0-9_$.]*(/[a-zA-Z_$][a-zA-Z0-9_$.]*)*)?$`)

func ValidateFieldPath(path string) error {
	if !fieldPathRegex.MatchString(path) {
		return fmt.Errorf("field must be a valid path: start with /, not end with /, and each segment must follow JavaScript variable naming rules")
	}
	return nil
}

// catalogRegex validates a catalog name.
//
// Format: "<segment>" or "<segment>/<segment>/...".
//
//   - segment first char: [a-zA-Z0-9_]
//   - segment body chars: [a-zA-Z0-9_.\-]
//   - segments separated by exactly one "/"
//   - no leading or trailing "/", no "//"
//
// Forbidden by design (each with a concrete reason):
//
//   - ":"   — Redis key delimiter, would split <prefix>:<catalog>:<suffix>
//   - "|"   — delta member delimiter "delta|{mergeType}|{path}|{tsSeq}"
//   - "(" / ")" — reserved as OSS catalog-encoding type markers (lower /
//     upper fast paths in encodeOssCatalogName)
//   - leading "-" or "." — first conflicts with shell flag parsing
//     (`s3cmd cp -tenant ...`); second is the Unix hidden-file convention
//   - "/" at start / end / doubled — produces ambiguous OSS paths
//   - "+", "=", "@", "~", "#", "&" — URL / base64-padding / shell
//     metacharacters; safe to forbid unless someone has a concrete need
//   - whitespace, control chars, non-ASCII — Unicode NFC/NFD divergence
//     across OSes makes Redis-string equality unreliable
//
// Internal "/" is intentionally allowed — it lets operators encode a
// hierarchy (e.g. "tenantA/users") that materialises as real virtual
// directories in OSS, enabling per-prefix lifecycle / billing / listing.
var catalogRegex = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.\-]*(/[a-zA-Z0-9_][a-zA-Z0-9_.\-]*)*$`)

func ValidateCatalog(catalog string) error {
	if !catalogRegex.MatchString(catalog) {
		return fmt.Errorf("invalid catalog %q: each segment must be ASCII [a-zA-Z0-9_][a-zA-Z0-9_.\\-]*; segments joined by single \"/\"; no leading/trailing/doubled \"/\"; \":\" \"|\" \"(\" \")\" forbidden", catalog)
	}
	return nil
}
