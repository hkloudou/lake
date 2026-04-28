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
