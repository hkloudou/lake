package storage

// OSS key layout: "{md5(catalog)[0:4]}/{encodedCatalog}/{filename}".
// MD5 prefix gives 65,536 hot-spot-free shards; encodedCatalog keeps
// each tenant's prefix human-readable for OSS LIST / lifecycle / billing
// at the catalog level. No depth-based directory sharding — object
// stores have no per-prefix object-count penalty.

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
	"github.com/hkloudou/lake/v3/internal/encrypt"
	"github.com/hkloudou/lake/v3/internal/index"
)

type ossStorage struct {
	name     string
	bucket   *oss.Bucket
	endpoint string
	aesKey   []byte
}

// OSSConfig holds OSS configuration.
type OSSConfig struct {
	Name      string
	Endpoint  string // e.g. "oss-cn-hangzhou"; "-internal" appended in FC
	Bucket    string
	AccessKey string
	SecretKey string
	AESKey    string
	Internal  bool // force internal endpoint suffix
}

// NewOSSStorage builds an OSS-backed Storage. The endpoint may be a
// short region ("oss-cn-hangzhou") which is expanded to a full URL; if
// the FC_REGION env var matches the endpoint and the endpoint is not
// already internal, "-internal" is appended automatically.
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
	return &ossStorage{
		name:     cfg.Name,
		bucket:   bucket,
		endpoint: endpoint,
		aesKey:   []byte(cfg.AESKey),
	}, nil
}

func (s *ossStorage) Put(ctx context.Context, key string, data []byte) error {
	enc, err := encrypt.Encrypt(data, s.aesKey)
	if err != nil {
		return err
	}
	return s.bucket.PutObject(key, bytes.NewReader(enc))
}

func (s *ossStorage) Get(ctx context.Context, key string) ([]byte, error) {
	r, err := s.bucket.GetObject(key)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return encrypt.Decrypt(data, s.aesKey)
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

// RedisPrefix is the deployment Name; tenancy is keyed by Name only,
// not backend type, so cache / seqid spaces stay consistent across
// configs.
func (s *ossStorage) RedisPrefix() string { return s.name }

func (s *ossStorage) MakeDeltaKey(catalog string, ts index.TimeSeqID, mergeType int) string {
	return fmt.Sprintf("%s/%s_%d.dat", encodeOssCatalogPath(catalog, 4), ts, mergeType)
}

func (s *ossStorage) MakeSnapKey(catalog string, stop index.TimeSeqID) string {
	return fmt.Sprintf("%s/%s.snap", encodeOssCatalogPath(catalog, 4), stop)
}

// encodeOssCatalogPath: "{md5[0:shardSize]}/{encodeOssCatalogName(catalog)}".
func encodeOssCatalogPath(catalog string, shardSize int) string {
	hash := md5.Sum([]byte(catalog))
	return hex.EncodeToString(hash[:])[0:shardSize] + "/" + encodeOssCatalogName(catalog)
}

// encodeOssCatalogName encodes a catalog name for OSS paths.
//
// Three forms, distinguished by a 1-byte type marker:
//   - "(<name>"   pure lowercase + safe chars (a-z 0-9 - _ / .)
//   - ")<NAME>"   pure uppercase + safe chars
//   - "<base32>"  mixed-case or non-ASCII (lowercased base32, no padding)
//
// The marker resolves the case-insensitive collision risk on filesystems
// like macOS HFS+/NTFS that may underlie a "file" backend.
func encodeOssCatalogName(catalog string) string {
	if isOssCaseSafe(catalog, 'a', 'z') {
		return "(" + catalog
	}
	if isOssCaseSafe(catalog, 'A', 'Z') {
		return ")" + catalog
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte(catalog)))
}

// isOssCaseSafe checks catalog uses only the given letter case plus
// digits and "-_/."; returns false on empty input.
func isOssCaseSafe(catalog string, lo, hi rune) bool {
	if catalog == "" {
		return false
	}
	for _, r := range catalog {
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
