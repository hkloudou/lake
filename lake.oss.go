package lake

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/hkloudou/lake/internal/xsync"
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

	// OSS客户端缓存相关字段
	ossClient   *oss.Client
	ossBucket   *oss.Bucket
	ossClientMu sync.RWMutex

	lock sync.Mutex
	// snapMetaTasker sync.Once
}

// 获取OSS Bucket客户端，支持连接复用
func (m *lakeEngine) getBucket() (*oss.Bucket, error) {
	// 首先尝试读锁获取已缓存的client
	m.ossClientMu.RLock()
	if m.ossBucket != nil && m.meta != nil {
		bucket := m.ossBucket
		m.ossClientMu.RUnlock()
		return bucket, nil
	}
	m.ossClientMu.RUnlock()

	// 需要创建新的client，获取写锁
	m.ossClientMu.Lock()
	defer m.ossClientMu.Unlock()

	// 双重检查，避免重复创建
	if m.ossBucket != nil && m.meta != nil {
		return m.ossBucket, nil
	}

	// 创建新的OSS客户端
	internalStr := ""
	if m.internal {
		internalStr = "-internal"
	}

	client, err := oss.New(fmt.Sprintf("https://%s%s.aliyuncs.com", m.meta.location, internalStr), m.meta.AccessKey, m.meta.SecretKey)
	if err != nil {
		return nil, err
	}

	bucketClient, err := client.Bucket(m.meta.Bucket)
	if err != nil {
		return nil, err
	}

	// 缓存客户端
	m.ossClient = client
	m.ossBucket = bucketClient

	return bucketClient, nil
}

// 废弃原来的newClient方法，改为调用getBucket
func (m *lakeEngine) newClient() *oss.Bucket {
	bucket, err := m.getBucket()
	if err != nil {
		panic(err)
	}
	return bucket
}

// 清除OSS客户端缓存（当meta配置变更时调用）
func (m *lakeEngine) clearOSSCache() {
	m.ossClientMu.Lock()
	defer m.ossClientMu.Unlock()
	m.ossClient = nil
	m.ossBucket = nil
}

func (m *lakeEngine) Config(meta Meta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	// 清除OSS客户端缓存和meta缓存
	m.clearOSSCache()
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
		fmt.Println(obj.location)
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
