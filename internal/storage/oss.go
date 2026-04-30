package storage

// OSS key layout: "{md5(catalog)[0:4]}/{encodedCatalog}/{filename}".
// MD5 prefix gives 65,536 hot-spot-free shards; encodedCatalog keeps
// each tenant's prefix human-readable for OSS LIST / lifecycle / billing.
//
// Bodies are stored RAW (no Lake-side gzip / AES). Use OSS SSE for
// at-rest encryption.

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/hkloudou/lake/v3/internal/index"
)

type ossStorage struct {
	name     string
	bucket   *oss.Bucket
	endpoint string
}

// OSSConfig holds OSS configuration.
type OSSConfig struct {
	Name      string
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Internal  bool
}

// NewOSSStorage builds an OSS-backed Storage.
func NewOSSStorage(cfg OSSConfig) (*ossStorage, error) {
	endpoint := cfg.Endpoint
	if cfg.Internal {
		endpoint += "-internal"
	}
	if !strings.HasPrefix(endpoint, "http") {
		if r := os.Getenv("FC_REGION"); r != "" && strings.Contains(endpoint, r) && !strings.Contains(endpoint, "-internal") {
			endpoint += "-internal"
		}
		endpoint = "https://" + endpoint + ".aliyuncs.com"
	}
	client, err := oss.New(endpoint, cfg.AccessKey, cfg.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("oss client: %w", err)
	}
	bucket, err := client.Bucket(cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("oss bucket: %w", err)
	}
	return &ossStorage{name: cfg.Name, bucket: bucket, endpoint: endpoint}, nil
}

func (s *ossStorage) Put(ctx context.Context, key string, data []byte) error {
	return s.bucket.PutObject(key, bytes.NewReader(data))
}

func (s *ossStorage) Get(ctx context.Context, key string) ([]byte, error) {
	r, err := s.bucket.GetObject(key)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (s *ossStorage) Delete(ctx context.Context, key string) error {
	if err := s.bucket.DeleteObject(key); err != nil {
		if e, ok := err.(oss.ServiceError); ok && (e.StatusCode == 404 || e.Code == "NoSuchKey") {
			return nil
		}
		return err
	}
	return nil
}

func (s *ossStorage) Exists(ctx context.Context, key string) (bool, error) {
	return s.bucket.IsObjectExist(key)
}

func (s *ossStorage) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	marker := ""
	for {
		res, err := s.bucket.ListObjects(oss.Prefix(prefix), oss.Marker(marker))
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, o := range res.Objects {
			keys = append(keys, o.Key)
		}
		if !res.IsTruncated {
			return keys, nil
		}
		marker = res.NextMarker
	}
}

func (s *ossStorage) RedisPrefix() string { return s.name }

// MakeDeltaKey: "{md5}/{encoded}/{uuid}.dat".
func (s *ossStorage) MakeDeltaKey(catalog, uuid string) string {
	return fmt.Sprintf("%s/%s.dat", encodeOssCatalogPath(catalog, 4), uuid)
}

// MakeSnapKey: "{md5}/{encoded}/{stopTsSeq}.snap".
func (s *ossStorage) MakeSnapKey(catalog string, stop index.TimeSeqID) string {
	return fmt.Sprintf("%s/%s.snap", encodeOssCatalogPath(catalog, 4), stop)
}

// PresignPut signs a PUT URL for the given key. User metadata is baked
// into the signature so the client MUST send the listed headers
// verbatim; this enforces self-describing OSS objects.
func (s *ossStorage) PresignPut(ctx context.Context, key string, opts PresignOptions) (PresignedUpload, error) {
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = 15 * 60 * 1e9 // 15 min in ns; converted to seconds below
	}
	signOpts := []oss.Option{}
	headers := map[string]string{}
	if opts.ContentType != "" {
		signOpts = append(signOpts, oss.ContentType(opts.ContentType))
		headers["Content-Type"] = opts.ContentType
	}
	for k, v := range opts.UserMetadata {
		signOpts = append(signOpts, oss.Meta(k, v))
		headers["x-oss-meta-"+strings.ToLower(k)] = v
	}
	url, err := s.bucket.SignURL(key, oss.HTTPPut, int64(ttl.Seconds()), signOpts...)
	if err != nil {
		return PresignedUpload{}, fmt.Errorf("sign url: %w", err)
	}
	return PresignedUpload{URL: url, Method: "PUT", Headers: headers}, nil
}

// encodeOssCatalogPath: "{md5[0:shardSize]}/{encodeOssCatalogName(catalog)}".
func encodeOssCatalogPath(catalog string, shardSize int) string {
	hash := md5.Sum([]byte(catalog))
	return hex.EncodeToString(hash[:])[0:shardSize] + "/" + encodeOssCatalogName(catalog)
}

// encodeOssCatalogName encodes a catalog name for OSS paths.
//
// Three forms with a 1-byte type marker resolving case-insensitive
// collisions on filesystems like macOS HFS+ underlying File backends:
//
//   - "(<name>"   pure lowercase (a-z 0-9 - _ / .)
//   - ")<NAME>"   pure uppercase
//   - "<base32>"  mixed-case or non-ASCII (lowercased base32, no padding)
func encodeOssCatalogName(catalog string) string {
	if isOssCaseSafe(catalog, 'a', 'z') {
		return "(" + catalog
	}
	if isOssCaseSafe(catalog, 'A', 'Z') {
		return ")" + catalog
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte(catalog)))
}

func isOssCaseSafe(s string, lo, hi rune) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= lo && r <= hi:
		case r >= '0' && r <= '9':
		case r == '-', r == '_', r == '/', r == '.':
		default:
			return false
		}
	}
	return true
}
