package utils

import (
	"fmt"
	"regexp"
)

// fieldPathRegex: JSON field path used in delta members.
//
//   - starts with "/", does not end with "/"
//   - each segment matches [a-zA-Z_$][a-zA-Z0-9_$.]*
//   - "|" forbidden — it is the delta-member delimiter
var fieldPathRegex = regexp.MustCompile(`^/([a-zA-Z_$][a-zA-Z0-9_$.]*(/[a-zA-Z_$][a-zA-Z0-9_$.]*)*)?$`)

func ValidateFieldPath(path string) error {
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
	if !storagePartRegex.MatchString(provider) {
		return fmt.Errorf(`invalid storage provider %q: ASCII [a-zA-Z0-9][a-zA-Z0-9._\-]* required ("/" ":" "|" forbidden)`, provider)
	}
	return nil
}

func ValidateStorageBucket(bucket string) error {
	if !storagePartRegex.MatchString(bucket) {
		return fmt.Errorf(`invalid storage bucket %q: ASCII [a-zA-Z0-9][a-zA-Z0-9._\-]* required ("/" ":" "|" forbidden)`, bucket)
	}
	return nil
}
