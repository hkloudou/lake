package index

import "github.com/redis/go-redis/v9"

// Index is Lake's Redis index: the per-catalog delta log, the snap pointers,
// the tsSeq allocator and the notify dedup records, all under one key prefix.
//
//	{prefix}:d:{catalog}   ZSet    delta log; score = ts + seq/1e6, member = [mergeType, path, tsSeq, uri]
//	{prefix}:s             Hash    catalog → [tsSeq, uri]; "{catalog}:rg" → removal generation
//	{prefix}:seq:{catalog} String  last issued "ts_seq" (7-day TTL)
//	{prefix}:n:{uri}       String  member committed for this write (dedup, 1-hour TTL)
//	{prefix}:m:{indicator} Hash    sample memo (owned by the lake package)
type Index struct {
	rdb    *redis.Client
	prefix string
}

func New(rdb *redis.Client, prefix string) *Index { return &Index{rdb: rdb, prefix: prefix} }

func (x *Index) Prefix() string { return x.prefix }

func (x *Index) deltaKey(catalog string) string { return x.prefix + ":d:" + catalog }
func (x *Index) snapsKey() string               { return x.prefix + ":s" }
func (x *Index) allocKey(catalog string) string { return x.prefix + ":seq:" + catalog }
func (x *Index) dedupKey(uri string) string     { return x.prefix + ":n:" + uri }

// SampleKey is the memo hash of one sample indicator (field = catalog).
func (x *Index) SampleKey(indicator string) string { return x.prefix + ":m:" + indicator }
