package merge

import "strings"

// ToGjsonPath converts a validated field path to gjson/sjson syntax: drop the
// leading "/", then "/" becomes ".". Validation forbids "." (and every other
// gjson metacharacter) inside a segment, so nothing needs escaping.
// "/" → "", "/user" → "user", "/user/profile" → "user.profile".
func ToGjsonPath(path string) string {
	return strings.ReplaceAll(strings.TrimPrefix(path, "/"), "/", ".")
}
