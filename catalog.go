package lake

import (
	"fmt"
	"log"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// data Catalog
type catalog struct {
	internal        bool
	StorageClass    StorageClass
	Endpoint        string
	accessKeyID     string
	accessKeySecret string
	Bucket          string
	// Client          *oss.Bucket
	// Path         string
	// BoxName string
	path string
}

func (m catalog) newClient() *oss.Bucket {
	internalStr := ""
	if m.internal {
		internalStr = "-internal"
	}
	client, err := oss.New(fmt.Sprintf("http://oss-%s%s.aliyuncs.com", m.Endpoint, internalStr), m.accessKeyID, m.accessKeySecret)
	if err != nil {
		log.Panicln(err)
	}
	bucketClient, err := client.Bucket(m.Bucket)
	if err != nil {
		log.Panicln(err)
	}
	return bucketClient
}

// func NewOssCatalog(
// 	internal bool,
// 	endpoint string, bucket, accessKeyID string, accessKeySecret string,
// 	// nameSpace string,
// 	path string,
// ) *catalog {
// 	return &catalog{
// 		internal:        internal,
// 		StorageClass:    StorageClassOSS,
// 		Endpoint:        endpoint,
// 		accessKeyID:     accessKeyID,
// 		accessKeySecret: accessKeySecret,
// 		Bucket:          bucket,
// 		// Client:          bucketClient,
// 		// path:    fmt.Sprintf("%s/%s/%s", strconv.FormatUint(uint64(crc32.ChecksumIEEE([]byte(nameSpace+boxName))), 16)[:4], nameSpace, boxName),
// 		// BoxName: boxName,
// 		path: strings.Trim(path, "/"),
// 	}
// }
