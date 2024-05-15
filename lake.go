package lake

import (
	"fmt"
	"os"
	"time"

	"github.com/hkloudou/xlib/collection"
	"github.com/hkloudou/xlib/xcolor"
	"github.com/hkloudou/xlib/xsync"
	"github.com/redis/go-redis/v9"
)

type fileType uint8

const (
	DATA fileType = 1
	SNAP fileType = 2
)

type Option struct {
	cacheLimit      int
	cacheTTL        time.Duration
	metaSnapTTL     time.Duration
	taskCleanWindow time.Duration
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

func WithMetaSnapTTL(duration time.Duration) func(l *Option) {
	return func(l *Option) {
		l.metaSnapTTL = duration
	}
}

func WithTaskClenTTL(duration time.Duration) func(l *Option) {
	return func(l *Option) {
		l.taskCleanWindow = duration
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
	tmp := &lakeEngine{
		rdb:      redis.NewClient(redisopt),
		barrier:  xsync.NewSingleFlight[Meta](),
		cache:    cache,
		internal: os.Getenv("FC_REGION") == "cn-hangzhou",
		prefix:   "cl:",
	}
	if options.metaSnapTTL != 0 {
		go func() {
			for {
				if tmp.meta == nil {
					time.Sleep(1 * time.Second)
					continue
				}
				err := tmp.snapMeta()
				if err != nil {
					fmt.Println(xcolor.Red("SnapMeta"), err.Error())
				}
				time.Sleep(options.metaSnapTTL)
			}
		}()
	}

	if options.taskCleanWindow != 0 {
		go func() {
			for {
				tmp.taskCleanignore(options.taskCleanWindow)
				time.Sleep(1 * time.Second)
			}
		}()
	}

	return tmp
}
