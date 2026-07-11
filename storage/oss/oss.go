// Package oss is an Aliyun OSS storage backend. One Client (one endpoint +
// credential set) vends many buckets and implements storage.Presigner.
//
//	client := oss.New(oss.Config{Endpoint: "oss-cn-hangzhou", AccessKey: ak, SecretKey: sk})
//	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
//	    if provider != "oss" { return nil, fmt.Errorf("unknown provider %q", provider) }
//	    return client.Bucket(bucket), nil
//	}
package oss

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	alioss "github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/hkloudou/lake/v3/storage"
)

// Config holds OSS connection settings.
type Config struct {
	Endpoint  string // e.g. "oss-cn-hangzhou" or a full https URL
	AccessKey string
	SecretKey string
	Internal  bool // append "-internal" to the endpoint (intra-VPC)
}

// Client is an endpoint+credential-scoped OSS handle that vends buckets.
type Client struct {
	cli     *alioss.Client
	mu      sync.Mutex
	buckets map[string]*alioss.Bucket
}

// New builds an OSS client. It returns an error only on malformed config; the
// connection is lazy.
//
// Endpoint forms: a region shorthand ("oss-cn-hangzhou" — Internal and
// FC_REGION handling apply, ".aliyuncs.com" is appended), a full host
// ("oss-cn-hangzhou-internal.aliyuncs.com"), or a full URL. Internal=true is
// only meaningful with the shorthand — combined with a host/URL it would
// have to rewrite an address the caller already spelled out, so that
// combination is rejected loudly instead of minting a host that fails DNS
// on the first request.
func New(cfg Config) (*Client, error) {
	endpoint := cfg.Endpoint
	switch {
	case strings.HasPrefix(endpoint, "http://"), strings.HasPrefix(endpoint, "https://"):
		if cfg.Internal && !strings.Contains(endpoint, "-internal") {
			return nil, fmt.Errorf("oss: Internal requires the region-shorthand Endpoint (e.g. %q); bake \"-internal\" into the full URL %q instead", "oss-cn-hangzhou", cfg.Endpoint)
		}
	case strings.Contains(endpoint, "."):
		// Full host without a scheme — never re-suffix ".aliyuncs.com".
		if cfg.Internal && !strings.Contains(endpoint, "-internal") {
			return nil, fmt.Errorf("oss: Internal requires the region-shorthand Endpoint (e.g. %q); bake \"-internal\" into the host %q instead", "oss-cn-hangzhou", cfg.Endpoint)
		}
		endpoint = "https://" + endpoint
	default:
		// Region shorthand.
		if !strings.Contains(endpoint, "-internal") {
			if cfg.Internal {
				endpoint += "-internal"
			} else if r := os.Getenv("FC_REGION"); r != "" && strings.Contains(endpoint, r) {
				endpoint += "-internal"
			}
		}
		endpoint = "https://" + endpoint + ".aliyuncs.com"
	}
	cli, err := alioss.New(endpoint, cfg.AccessKey, cfg.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("oss: client: %w", err)
	}
	return &Client{cli: cli, buckets: map[string]*alioss.Bucket{}}, nil
}

// Bucket returns a bucket-scoped storage.Storage (also a Presigner). The
// *oss.Bucket handle is created lazily and cached per name.
func (c *Client) Bucket(name string) storage.Storage { return &bucket{c: c, name: name} }

func (c *Client) handle(name string) (*alioss.Bucket, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.buckets[name]; ok {
		return b, nil
	}
	b, err := c.cli.Bucket(name)
	if err != nil {
		return nil, fmt.Errorf("oss: bucket %s: %w", name, err)
	}
	c.buckets[name] = b
	return b, nil
}

type bucket struct {
	c    *Client
	name string
}

func (b *bucket) Get(ctx context.Context, _ /*catalog*/, path string) ([]byte, error) {
	h, err := b.c.handle(b.name)
	if err != nil {
		return nil, err
	}
	r, err := h.GetObject(path, alioss.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("oss: get %s: %w", path, err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (b *bucket) Put(ctx context.Context, _ /*catalog*/, path string, data []byte) error {
	h, err := b.c.handle(b.name)
	if err != nil {
		return err
	}
	return h.PutObject(path, bytes.NewReader(data), alioss.WithContext(ctx))
}

// PresignPut signs a PUT URL. UserMetadata is baked into the signature, so the
// client MUST send the listed headers verbatim — this keeps the OSS object
// self-describing.
func (b *bucket) PresignPut(_ context.Context, _ /*catalog*/, path string, opts storage.PresignOptions) (storage.PresignedUpload, error) {
	h, err := b.c.handle(b.name)
	if err != nil {
		return storage.PresignedUpload{}, err
	}
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	// SignURL takes whole seconds; a sub-second TTL would truncate to 0 and
	// sign a URL that is already expired when returned.
	expireSecs := int64(ttl / time.Second)
	if expireSecs < 1 {
		expireSecs = 1
	}
	signOpts := []alioss.Option{}
	headers := map[string]string{}
	if opts.ContentType != "" {
		signOpts = append(signOpts, alioss.ContentType(opts.ContentType))
		headers["Content-Type"] = opts.ContentType
	}
	for k, v := range opts.UserMetadata {
		signOpts = append(signOpts, alioss.Meta(k, v))
		headers["x-oss-meta-"+strings.ToLower(k)] = v
	}
	url, err := h.SignURL(path, alioss.HTTPPut, expireSecs, signOpts...)
	if err != nil {
		return storage.PresignedUpload{}, fmt.Errorf("oss: sign url: %w", err)
	}
	return storage.PresignedUpload{URL: url, Method: "PUT", Headers: headers}, nil
}
