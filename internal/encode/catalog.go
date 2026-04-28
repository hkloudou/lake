package encode

// EncodeRedisCatalogName is the canonical hook for transforming a catalog name
// (or other user-supplied identifier such as a sample indicator) before it is
// embedded inside a Redis key.
//
// At present the function is the identity — Redis keys store catalog names
// verbatim. Callers must therefore ensure that catalog names do not contain
// characters that would clash with the Redis key delimiter ":" or the delta
// member delimiter "|".
//
// This function is preserved as the single chokepoint for any future encoding
// scheme (base32, hex, base64-url, etc.). Adding encoding back means changing
// this function and ensuring any reverse lookup also goes through it.
func EncodeRedisCatalogName(s string) string {
	return s
}
