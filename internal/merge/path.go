package merge

import (
	"fmt"
	"regexp"
	"strings"
)

// fieldPathRegex validates that field path:
// - Starts with /
// - Does not end with /
// - Each segment follows JavaScript variable naming rules (starts with letter/_/$, followed by letters/digits/_/$/.)
var fieldPathRegex = regexp.MustCompile(`^/([a-zA-Z_$][a-zA-Z0-9_$.]*(/[a-zA-Z_$][a-zA-Z0-9_$.]*)*)?$`)

func ValidateFieldPath(path string) error {
	if !fieldPathRegex.MatchString(path) {
		return fmt.Errorf("field must be a valid path: start with /, not end with /, and each segment must follow JavaScript variable naming rules")
	}
	return nil
}

// toGjsonPath converts a field path to gjson path format
// It splits the path by "/" and escapes each segment for gjson usage
// Examples:
//   - "/" -> ""
//   - "/user" -> "user"
//   - "/user/profile" -> "user.profile"
//   - "/user.info" -> "user\.info" (dots in field names are escaped)
//   - "/user.info/profile.data" -> "user\.info.profile\.data"
func ToGjsonPath(path string) string {
	// Remove leading /
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	// If empty (root path "/"), return empty string
	if path == "" {
		return ""
	}

	// Split by / and escape each segment
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		segments[i] = escapeGjsonKey(seg)
	}

	// Join with .
	return strings.Join(segments, ".")
}

// escapeGjsonKey escapes special characters in a gjson key
// Based on gjson's internal escapeComp and isSafePathKeyChar functions
func escapeGjsonKey(key string) string {
	for i := 0; i < len(key); i++ {
		if !isSafeGjsonChar(key[i]) {
			// Found a character that needs escaping
			escaped := make([]byte, 0, len(key)+8) // pre-allocate with some extra space
			escaped = append(escaped, key[:i]...)
			for ; i < len(key); i++ {
				if !isSafeGjsonChar(key[i]) {
					escaped = append(escaped, '\\')
				}
				escaped = append(escaped, key[i])
			}
			return string(escaped)
		}
	}
	return key
}

// isSafeGjsonChar returns true if the character is safe for gjson paths
// Safe characters: a-z, A-Z, 0-9, _, $, -, :
// Based on gjson's isSafePathKeyChar plus $ for our use case
func isSafeGjsonChar(c byte) bool {
	return c == '_' || c == '$' || c == '-' || c == ':' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}
