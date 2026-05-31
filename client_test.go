package lake

import (
	"time"

	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
	"github.com/redis/go-redis/v9"
)

// memResolver returns a storage.Resolver backed by one in-memory Store (every
// provider/bucket shares it). Enough for tests that don't presign or that
// drive storage directly.
func memResolver() storage.Resolver {
	st := mem.New()
	return func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return st.Bucket(bucket), nil
	}
}

// newTestClientRDB builds a Client on the given index Redis with prefix "test"
// and an in-memory storage resolver.
func newTestClientRDB(rdb *redis.Client) *Client {
	return New("test", rdb, memResolver())
}

// newTestClient builds a test Client pointed at a Redis addr (often
// unreachable, to exercise error paths) with a 200ms dial timeout.
func newTestClient(addr string) *Client {
	return newTestClientRDB(redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 200 * time.Millisecond}))
}
