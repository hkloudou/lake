package lake

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/hkloudou/xlib/collection"
	"github.com/hkloudou/xlib/xsync"
	"github.com/redis/go-redis/v9"
)

type lakeEngine struct {
	// redisOption *redis.Options
	rdb      *redis.Client
	barrier  xsync.SingleFlight[Meta]
	meta     *Meta
	internal bool
	cache    *collection.Cache[map[string]any]
	prefix   string
}

// func (m *lakeEngine) Write
func (m lakeEngine) newClient() *oss.Bucket {
	internalStr := ""
	if m.internal {
		internalStr = "-internal"
	}
	client, err := oss.New(fmt.Sprintf("https://%s%s.aliyuncs.com", m.meta.location, internalStr), m.meta.AccessKey, m.meta.SecretKey)
	if err != nil {
		panic(err)
	}

	bucketClient, err := client.Bucket(m.meta.Bucket)
	if err != nil {
		panic(err)
	}
	return bucketClient
}

func (m *lakeEngine) Config(meta Meta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	m.meta = nil
	return m.rdb.Set(context.TODO(), "lake.setting", data, 0).Err()
}

func (m *lakeEngine) readMeta() error {
	if m.meta != nil {
		return nil
	}
	meta, err := m.barrier.Do("lake.setting", func() (Meta, error) {
		var obj Meta
		err := m.rdb.Get(context.TODO(), "lake.setting").Scan(&obj)
		if err != nil {
			return obj, err
		}
		client, err := oss.New("https://oss-cn-hangzhou.aliyuncs.com", obj.AccessKey, obj.SecretKey)
		if err != nil {
			return obj, err
		}
		location, err := client.GetBucketLocation(obj.Bucket)
		if err != nil {
			return obj, err
		}
		obj.location = location
		// fmt.Println(obj.location)
		return obj, err
	})
	if err != nil {
		m.meta = nil
		return err
	}
	m.meta = &meta
	return nil
}

func NewLake(metaUrl string,
) *lakeEngine {
	opt, err := redis.ParseURL(metaUrl)
	if err != nil {
		panic(err)
	}
	cache, err := collection.NewCache[map[string]any](1*time.Second, collection.WithLimit[map[string]any](1000))
	if err != nil {
		panic(err)
	}
	return &lakeEngine{
		rdb:      redis.NewClient(opt),
		barrier:  xsync.NewSingleFlight[Meta](),
		cache:    cache,
		internal: os.Getenv("FC_REGION") == "cn-hangzhou",
		prefix:   "cl:",
	}
}
