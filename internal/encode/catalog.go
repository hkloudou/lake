// Package encode is the single chokepoint for transforming user-supplied
// identifiers (catalog name, sample indicator) before they are embedded
// in Redis keys. Currently the identity function — Redis stores names
// verbatim. To introduce encoding (base32, hex, etc.), change this one
// function; all call sites already route through it.
package encode

func EncodeRedisCatalogName(s string) string { return s }
