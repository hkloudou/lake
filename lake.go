package lake

import (
	"os"
	"time"

	"github.com/hkloudou/xlib/collection"
	"github.com/hkloudou/xlib/xsync"
	"github.com/redis/go-redis/v9"
)

type fileType uint8

const (
	DATA fileType = 1
	SNAP fileType = 2
)

type Option struct {
	cacheLimit int
	cacheTTL   time.Duration
}

func WithCacheLimit(limit int) func(l *Option) {
	return func(l *Option) {
		l.cacheLimit = limit
	}
}

func WithCacheTTL(duration time.Duration) func(l *Option) {
	return func(l *Option) {
		l.cacheTTL = duration
	}
}

func NewLake(metaUrl string, opts ...func(*Option)) *lakeEngine {
	redisopt, err := redis.ParseURL(metaUrl)
	if err != nil {
		panic(err)
	}
	var options = Option{
		cacheTTL:   24 * time.Hour,
		cacheLimit: 1000,
	}
	for _, opt := range opts {
		opt(&options)
	}

	cache, err := collection.NewCache[any](options.cacheTTL, collection.WithLimit[any](options.cacheLimit))
	if err != nil {
		panic(err)
	}

	return &lakeEngine{
		rdb:      redis.NewClient(redisopt),
		barrier:  xsync.NewSingleFlight[Meta](),
		cache:    cache,
		internal: os.Getenv("FC_REGION") == "cn-hangzhou",
		prefix:   "cl:",
	}
}
