package storage

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"

	"github.com/hkloudou/lake/v2/internal/index"
)

// func getTimeYm(ts index.TimeSeqID) string {
// 	return time.Unix(ts.Timestamp, 0).In(time.UTC).Format("200601")
// }

// getTimeHash returns 3-level hash path for file sharding
// hash1: 00-ff (256 dirs), hash2: 00-ff (256 dirs), hash3: 00-ff (256 dirs)
// Total: 256 × 256 × 256 = 16,777,216 leaf directories
// Hash cycles every ~194 days, but timestamp in filename prevents collision
func getTimeHash(ts index.TimeSeqID) (hash1, hash2, hash3 string) {
	hash := fmt.Sprintf("%06x", ts.Timestamp&0xffffff) // Low 24 bits
	return hash[0:2], hash[2:4], hash[4:6]
}

// getCatalogMd5Prefix returns first 2 chars of catalog MD5 for file storage sharding
// Returns: 256 possible values (00-ff)
func getCatalogMd5Prefix0xff(catalog string) string {
	hash := md5.Sum([]byte(catalog))
	return hex.EncodeToString(hash[:])[0:2]
}
