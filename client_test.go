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
// unreachable, to exercise error paths) with a 200ms dial timeout and both
// go-redis retry loops disabled so error-path tests fail fast: MaxRetries -1
// stops command retries (3 with backoff by default), and DialerRetries 1
// allows exactly one dial attempt — 0 and negatives mean "use default 5",
// each retry adding a 100ms backoff.
func newTestClient(addr string) *Client {
	return newTestClientRDB(redis.NewClient(&redis.Options{
		Addr:          addr,
		DialTimeout:   200 * time.Millisecond,
		MaxRetries:    -1,
		DialerRetries: 1,
	}))
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

// newDeadClientOpts is newDeadClient for tests that need a custom resolver
// and/or client options (e.g. a failing snap target) instead of the plain
// mem-backed default.
func newDeadClientOpts(t *testing.T, resolve storage.Resolver, opts ...func(*option)) *Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: unreachableRedis, DialTimeout: 200 * time.Millisecond, MaxRetries: -1, DialerRetries: 1})
	t.Cleanup(func() { _ = rdb.Close() })
	return New("test", rdb, resolve, opts...)
}
