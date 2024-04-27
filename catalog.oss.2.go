package lake

import (
	"encoding/json"
	"path"
	"sort"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// ossFileProperty properties of an OSS file
/*
File: ${unix}_${%06d:seq_id}_${uuid}.${format}
SNAP: ${unix}_${%06d:seq_id}_${uuid}.snap
*/

type ossFileProperty struct {
	Property oss.ObjectProperties
	Format   TextFormat
	Unix     int64
	SeqID    int64
}

func (o ossFileProperty) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{o.Format, o.Unix, o.SeqID, o.Property.Key})
}

// sort by Unix,SeqID
type ossFilePropertySlice []ossFileProperty

func (a ossFilePropertySlice) Len() int      { return len(a) }
func (a ossFilePropertySlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ossFilePropertySlice) Less(i, j int) bool {
	if a[i].Unix == a[j].Unix {
		return a[i].SeqID < a[j].SeqID
	}
	return a[i].Unix < a[j].Unix
}

func (m catalog) ListOssFiles(sampleUnix int64) (ossFilePropertySlice, error) {
	items, err := m.newClient().ListObjectsV2(
		oss.Prefix(m.path),
		oss.MaxKeys(500),
		oss.ObjectStorageClass(oss.StorageStandard),
	)
	if err != nil {
		return nil, err
	}
	var result = make(ossFilePropertySlice, 0)
	for i := 0; i < len(items.Objects); i++ {
		obj := items.Objects[i]
		unixTime := getNumericPart(path.Base(obj.Key), 0)
		if unixTime > (sampleUnix) {
			continue
		}
		if strings.HasSuffix(obj.Key, ".snap") {
			result = append(result, ossFileProperty{
				Property: obj,
				Format:   TextFormatSNAP,
				Unix:     getNumericPart(path.Base(obj.Key), 0),
				SeqID:    0,
			})
		} else if strings.HasSuffix(obj.Key, ".json") {
			result = append(result, ossFileProperty{
				Property: obj,
				Format:   TextFormatJSON,
				Unix:     getNumericPart(path.Base(obj.Key), 0),
				SeqID:    getNumericPart(path.Base(obj.Key), 1),
			})
		}
	}
	sort.Sort(result)
	return result, nil
}
