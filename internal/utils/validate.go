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
