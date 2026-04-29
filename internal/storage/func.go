package storage

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/index"
)

// timeHash splits the low 24 bits of the timestamp into a 3-level
// "{ab}/{cd}/{ef}" path. Cycles every ~194 days; the full timestamp in
// the filename keeps file paths unique across cycles.
func timeHash(ts index.TimeSeqID) (string, string, string) {
	h := fmt.Sprintf("%06x", ts.Timestamp&0xffffff)
	return h[0:2], h[2:4], h[4:6]
}

// catalogMd5Prefix2 returns the first 2 hex chars of md5(catalog).
func catalogMd5Prefix2(catalog string) string {
	hash := md5.Sum([]byte(catalog))
	return hex.EncodeToString(hash[:])[0:2]
}
