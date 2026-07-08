package merge

import (
	"strings"
)

// ToGjsonPath converts a field path to gjson path format in a single pass:
// the leading "/" is dropped, segment separators "/" become ".", and any
// character gjson treats specially (only "." can actually occur — validation
// pins segments to [a-zA-Z0-9_$.]) is escaped with a backslash.
// Examples:
//   - "/" -> ""
//   - "/user" -> "user"
//   - "/user/profile" -> "user.profile"
//   - "/user.info" -> "user\.info" (dots in field names are escaped)
//   - "/user.info/profile.data" -> "user\.info.profile\.data"
//
// Runs once per delta per read, so the no-escape common case returns the
// input substring without allocating.
func ToGjsonPath(path string) string {
	// Remove leading /
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	// If empty (root path "/"), return empty string
	if path == "" {
		return ""
	}

	rewrite := false
	for i := 0; i < len(path); i++ {
		if c := path[i]; c == '/' || !isSafeGjsonChar(c) {
			rewrite = true
			break
		}
	}
	if !rewrite {
		return path
	}

	var b strings.Builder
	b.Grow(len(path) + 8)
	for i := 0; i < len(path); i++ {
		switch c := path[i]; {
		case c == '/':
			b.WriteByte('.')
		case !isSafeGjsonChar(c):
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
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
