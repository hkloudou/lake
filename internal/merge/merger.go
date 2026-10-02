// Package merge replays a catalog's delta log onto a base document. Two
// strategies exist: Replace (set a subtree verbatim) and RFC 7396 JSON Merge
// Patch.
package merge
