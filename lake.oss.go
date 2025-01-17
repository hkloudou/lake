package lake

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/hkloudou/xlib/xsync"
	"github.com/redis/go-redis/v9"
)

type lakeEngine struct {
	// redisOption *redis.Options
	rdb      *redis.Client
	barrier  xsync.SingleFlight[Meta]
	meta     *Meta
	internal bool

	cache              Cache //*collection.Cache[any]
	prefix             string
	keyTaskProd        string
	keyTaskCleanIgnore string

	lock sync.Mutex
	// snapMetaTasker sync.Once
}

// func (m *lakeEngine) Write
func (m *lakeEngine) newClient() *oss.Bucket {
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
		internalStr := ""
		if m.internal {
			internalStr = "-internal"
		}
		client, err := oss.New("https://oss-cn-hangzhou"+internalStr+".aliyuncs.com", obj.AccessKey, obj.SecretKey)
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
	m.prefix = fmt.Sprintf("%s:%s:d:", meta.Storage, meta.Bucket)
	m.keyTaskProd = fmt.Sprintf("%s:%s:task_prod", meta.Storage, meta.Bucket)
	m.keyTaskCleanIgnore = fmt.Sprintf("%s:%s:task_clean_ignore", meta.Storage, meta.Bucket)
	m.meta = &meta

	return nil
}
