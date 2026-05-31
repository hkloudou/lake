package lake

import (
	"testing"
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

// unreachableRedis is a closed-port addr: any Redis op against it fails fast
// (200ms dial timeout), so tests can exercise validation / event-emission /
// early-return paths that run before — or regardless of — the Redis call.
const unreachableRedis = "127.0.0.1:1"

// newDeadClient builds a Client whose index Redis is unreachable. It is the
// single constructor for the many tests that only need an initialized Client
// to reach a non-Redis code path (validation, emitted events, flight wiring).
func newDeadClient(t *testing.T) *Client {
	t.Helper()
	return newTestClient(unreachableRedis)
}
