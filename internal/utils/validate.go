package utils

import (
	"fmt"
	"regexp"
)

// fieldPathRegex validates that field path:
//   - Starts with /
//   - Does not end with /
//   - Each segment follows JavaScript variable naming rules (starts with letter/_/$, followed by letters/digits/_/$/.)
//   - CRITICAL: Does NOT allow | (pipe) character - it's used as member delimiter in Redis
//     Member format: delta|{mergeType}|{fieldPath}|{tsSeq}
//     If | was allowed in fieldPath, strings.Split would break member parsing
var fieldPathRegex = regexp.MustCompile(`^/([a-zA-Z_$][a-zA-Z0-9_$.]*(/[a-zA-Z_$][a-zA-Z0-9_$.]*)*)?$`)

// filePathRegex validates file path (allows segments starting with digits):
//   - Starts with /
//   - Does not end with /
//   - Each segment can contain letters, digits, _, $, . (can start with any of these including digits)
var filePathRegex = regexp.MustCompile(`^/([a-zA-Z0-9_$.]+(/[a-zA-Z0-9_$.]+)*)?$`)

func ValidateFieldPath(path string) error {
	if !fieldPathRegex.MatchString(path) {
		return fmt.Errorf("field must be a valid path: start with /, not end with /, and each segment must follow JavaScript variable naming rules")
	}
	return nil
}

func ValidateFilePath(path string) error {
	if !filePathRegex.MatchString(path) {
		return fmt.Errorf("file must be a valid path: start with /, not end with /, and each segment can be alphanumeric with _$. characters")
	}
	return nil
}
