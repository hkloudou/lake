package lake

import (
	"fmt"
	"log"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// data Catalog
type catalog struct {
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
	client, err := oss.New(fmt.Sprintf("http://oss-%s.aliyuncs.com", m.Endpoint), m.accessKeyID, m.accessKeySecret)
	if err != nil {
		log.Panicln(err)
	}
	bucketClient, err := client.Bucket(m.Bucket)
	if err != nil {
		log.Panicln(err)
	}
	return bucketClient
}

func NewOssCatalog(
	endpoint string, bucket, accessKeyID string, accessKeySecret string,
	// nameSpace string,
	path string,
) (*catalog, error) {
	// if !regexp.MustCompile(`^[1|5|9][1|2|3]\d{6}[^_IOZSVa-z\W]{10}$`).MatchString(creditCode) {
	// 	return nil, fmt.Errorf("invalid credit code")
	// }
	if strings.HasPrefix(path, "/") || strings.HasSuffix(path, "/") {
		return nil, fmt.Errorf("path should not start with / or end with /")
	}

	return &catalog{
		StorageClass:    StorageClassOSS,
		Endpoint:        endpoint,
		accessKeyID:     accessKeyID,
		accessKeySecret: accessKeySecret,
		Bucket:          bucket,
		// Client:          bucketClient,
		// path:    fmt.Sprintf("%s/%s/%s", strconv.FormatUint(uint64(crc32.ChecksumIEEE([]byte(nameSpace+boxName))), 16)[:4], nameSpace, boxName),
		// BoxName: boxName,
		path: path,
	}, nil
}
