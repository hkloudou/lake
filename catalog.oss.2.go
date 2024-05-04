package lake

import (
	"encoding/json"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// ossFileProperty properties of an OSS file
/*
File: ${dataunix}_${%06d:seq_id}_${merge}_${uuid}.${format}
SNAP: ${lastdataunix}_${sampleunix}.snap
*/

type OssDataResult struct {
	Data             map[string]any       `json:"data"`
	Files            ossFilePropertySlice `json:"-"` // [ignore, format, unix, seqid, merge, uuid, key
	LastModifiedUnix int64                `json:"lastModifiedUnix"`
	SampleUnix       int64                `json:"sampleUnix"`
	LastSnap         *ossFileProperty     `json:"-"`
}

func (o OssDataResult) ShouldSnap(window time.Duration) bool {
	return o.SampleUnix-o.LastModifiedUnix > int64(window.Seconds()) && (o.LastSnap == nil || o.LastSnap.Unix != o.LastModifiedUnix)
}

type ossFileProperty struct {
	property oss.ObjectProperties
	Format   TextFormat

	Field []string

	Unix    int64
	SeqID   int64
	Merge   MergeType
	UUID    string
	Ignore  bool
	Fetched bool

	Value any
}

func (o ossFileProperty) MarshalJSON() ([]byte, error) {
	// o.Property.
	return json.Marshal([]any{!o.Ignore, o.Format, o.Unix, o.SeqID, o.Merge, o.UUID, o.property.Key})
}

func (o ossFileProperty) UnmarshalJSON(data []byte) error {
	// var obj []interface{}
	// return json.Marshal([]any{!o.Ignore, o.Format, o.Unix, o.SeqID, o.Merge, o.UUID, o.Property.Key})
	return nil
}

// sort by Unix,SeqID
type ossFilePropertySlice []ossFileProperty

func (m ossFilePropertySlice) Len() int      { return len(m) }
func (m ossFilePropertySlice) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m ossFilePropertySlice) Less(i, j int) bool {
	if m[i].Unix == m[j].Unix {
		if m[i].Format == TextFormatSNAP && m[j].Format != TextFormatSNAP {
			return false
		} else if m[j].Format == TextFormatSNAP && m[i].Format != TextFormatSNAP {
			return true
		}
		return m[i].SeqID < m[j].SeqID
	}
	return m[i].Unix < m[j].Unix
}

func (m ossFilePropertySlice) LastSnap() *ossFileProperty {
	for i := len(m) - 1; i >= 0; i-- {
		if m[i].Format == TextFormatSNAP {
			return &m[i]
		}
	}
	return nil
}

func (m ossFilePropertySlice) LastUnix() int64 {
	for i := len(m) - 1; i >= 0; i-- {
		if !m[i].Ignore {
			return m[i].Unix
		}
	}
	return 0
}

func (m ossFilePropertySlice) Fetch(c *catalog) error {
	var wg = sync.WaitGroup{}
	var lastError error
	for i := 0; i < len(m); i++ {
		if m[i].Ignore || m[i].Fetched {
			continue
		}
		wg.Add(1)
		go func(i2 int) {
			defer wg.Done()
			file := m[i2]
			// fmt.Println("read", file.Property.Key)
			buffer, err := c.newClient().GetObject(file.property.Key)
			if err != nil {
				lastError = err
				return
			}
			data, err := io.ReadAll(buffer)
			if err != nil {
				lastError = err
				return
			}
			// var tmp = ossDataResult{}
			// if file.Format == TextFormatSNAP || file.Format == TextFormatJSON {
			// var tmp OssDataResult
			// err = json.Unmarshal(data, &tmp)
			// if err != nil {
			// 	lastError = err
			// 	return
			// }
			// m[i2].Value = tmp
			// fmt.Println("snap", tmp)
			// }
			if file.Format == TextFormatSNAP || file.Format == TextFormatJSON {
				var tmp any
				err = json.Unmarshal(data, &tmp)
				if err != nil {
					lastError = err
					return
				}
				m[i2].Value = tmp
			}
			m[i2].Fetched = true
		}(i)
	}
	wg.Wait()
	return lastError
}

func (m ossFilePropertySlice) Merga() *OssDataResult {
	// var result = make(map[string]any, 0)

	result := OssDataResult{
		Data:             map[string]any{},
		Files:            m,
		LastModifiedUnix: 0,
		// SampleUnix:       sampleUnix,
	}
	for i := 0; i < len(m); i++ {
		// fmt.Println(!m[i].Ignore, m[i].Value)
		if m[i].Ignore {
			continue
		}
		switch m[i].Value.(type) {
		// case OssDataResult:
		// 	result = (m[i].Value.(OssDataResult))
		// 	// result.SampleUnix = time.Now().Unix()
		// 	result.Files = m
		// fmt.Println("SNAP", m[i].Field, m[i].Value)
		default:
			// fmt.Println("type", m[i].Value))
			// fmt.Println("JSON", m[i].Field, m[i].Value)
			// fmt.Println(!m[i].Ignore, m[i].Value)
			updateResult(&result.Data, &m[i])
			if m[i].Unix > result.LastModifiedUnix {
				result.LastModifiedUnix = m[i].Unix
			}
		}
	}
	// if result.SampleUnix == 0 {
	result.SampleUnix = time.Now().Unix()
	// }
	result.LastSnap = m.LastSnap()
	return &result
}

// func (m ossFilePropertySlice) RemoveOld(c *catalog) {
// 	lastSnap := m.LastSnap()
// 	if lastSnap != nil {
// 		var deleteList = make([]string, 0)
// 		for i := 0; i < len(m); i++ {
// 			if len(m[i].Property.Key) == 0 {
// 				panic("key is empty")
// 			}
// 			if m[i].Unix <= lastSnap.Unix && len(lastSnap.Property.Key) > 0 && m[i].Property.Key != lastSnap.Property.Key {
// 				deleteList = append(deleteList, m[i].Property.Key)
// 			}
// 		}
// 		// fmt.Println("deleteList", deleteList)
// 		// if len(deleteList) > 0 {
// 		// 	c.newClient().PutObjectTagging(deleteList)
// 		// }
// 		for i := 0; i < len(deleteList); i++ {
// 			c.newClient().PutObjectTagging(deleteList[i], oss.Tagging{
// 				Tags: []oss.Tag{
// 					{Key: "hkloudou.lake-deleting", Value: "true"},
// 				},
// 			})
// 		}
// 	}
// }

func (m catalog) ListOssFiles() (ossFilePropertySlice, error) {
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
		fileName := path.Base(obj.Key)
		parts := strings.Split(strings.ReplaceAll(fileName, ".", "_"), "_")
		pathSplit := strings.Split(strings.Trim(strings.Replace(obj.Key, m.path, "", 1), "/"), "/")

		if strings.HasSuffix(obj.Key, ".snap") {
			result = append(result, ossFileProperty{
				property: obj,
				Format:   TextFormatSNAP,
				Field:    nil,
				Unix:     getSliceNumericPart(parts, 0),
				SeqID:    getSliceNumericPart(parts, 1),
				Merge:    MergeTypeOver,
			})
		} else if strings.HasSuffix(obj.Key, ".json") {
			result = append(result, ossFileProperty{
				property: obj,
				Format:   TextFormatJSON,

				Field: pathSplit[:len(pathSplit)-1],

				Unix:  getSliceNumericPart(parts, 0),
				SeqID: getSliceNumericPart(parts, 1),
				Merge: MergeType(getSliceNumericPart(parts, 2)),
				UUID:  parts[3],
			})
		}
	}
	sort.Sort(result)
	lastSnap := result.LastSnap()
	if lastSnap != nil {
		for i := 0; i < len(result); i++ {
			//ignore data lte snaptime
			if result[i].Unix <= lastSnap.Unix && result[i].property.Key != lastSnap.property.Key {
				result[i].Ignore = true
			}
		}
	}
	return result, nil
}
