package lake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/google/uuid"
)

type DataItem[T any] struct {
	// Key              string
	Value            T
	Files            []string
	LastModifiedUnix int64
}

type SourceFile struct {
	Prefix       string
	FileName     string
	FullPath     string
	Merge        int
	Type         string //`xml:"Type"` // Object type
	Size         int64  //`xml:"Size"` // Object size
	ETag         string //`xml:"ETag"` // Object ETag
	LastModified time.Time

	Field []string
	// Data  []byte
	Value map[string]interface{}
}

func (m catalog) WriteJsonData(merge int, reqid int, field string, data []byte) error {
	if merge != 0 && merge != 1 {
		return fmt.Errorf("unknown merge")
	}
	arr := strings.Trim(field, ".")
	fieldPath := ""
	if arr != "" {
		fieldPath = strings.ReplaceAll(arr, ".", "/") + "/"
	}
	return m.newClient().PutObject(fmt.Sprintf("%s/data/%s%06d_%s_%d.json", m.path, fieldPath, reqid, uuid.New().String(), merge), bytes.NewReader(data))
}

func (m catalog) BuildData(before time.Time) (map[string]any, []*SourceFile, error) {
	//list information
	snaps := make([]oss.ObjectProperties, 0)
	jsons := make([]oss.ObjectProperties, 0)
	//split data
	if items, err := m.newClient().ListObjectsV2(
		oss.Prefix(m.path),
		oss.MaxKeys(500),
		oss.ObjectStorageClass(oss.StorageStandard),
	); err != nil {
		return nil, nil, err
	} else {
		for i := 0; i < len(items.Objects); i++ {
			obj := items.Objects[i]
			if obj.LastModified.UnixNano() > (before.UnixNano()) {
				continue
			}
			if strings.HasSuffix(obj.Key, ".snap") {
				snaps = append(snaps, obj)
			} else if strings.HasSuffix(obj.Key, ".json") {
				jsons = append(jsons, obj)
			}
		}
	}

	var lock = sync.RWMutex{}
	// var result = make(map[string]*DataItem[map[string]any])
	var wg = sync.WaitGroup{}
	var lastError error
	var snapObject = make(map[string]any, 0)
	if len(snaps) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sort.Slice(snaps, func(i, j int) bool {
				return snaps[i].LastModified.UnixNano() < snaps[j].LastModified.UnixNano()
			})
			lastSnap := snaps[len(snaps)-1]
			// bucket.GetObject(lastSnap.Key)
			buffer, err := m.newClient().GetObject(lastSnap.Key)
			if err != nil {
				lastError = err
				return
			}
			data, err := io.ReadAll(buffer)
			if err != nil {
				lastError = err
				return
			}
			err = json.Unmarshal(data, &snapObject)
			if err != nil {
				lastError = err
				return
			}
		}()
	}
	var files = make([]*SourceFile, 0)
	for i := 0; i < len(jsons); i++ {
		wg.Add(1)
		go func(i2 int) {
			// fmt.Println(i2)
			defer wg.Done()
			obj, err := m.readOssSourceFile(jsons[i2])
			if err != nil {
				lastError = err
				return
			}
			if obj == nil {
				return
			}
			lock.Lock()
			files = append(files, obj)
			lock.Unlock()
		}(i)
	}

	// getNumericPart 解析文件名中的数字部分
	getNumericPart := func(filename string) int {
		parts := strings.Split(filename, "_")
		if len(parts) > 0 {
			num, err := strconv.Atoi(parts[0])
			if err == nil {
				return num
			}
		}
		return 0 // 如果解析失败，返回0（或者可以选择处理错误）
	}
	wg.Wait()
	sort.Slice(files, func(i, j int) bool {
		if files[i].LastModified.UnixNano() == files[j].LastModified.UnixNano() {
			// 如果时间相同，则按文件名排序
			return getNumericPart(files[i].FileName) < getNumericPart(files[j].FileName)
		}
		return files[i].LastModified.UnixNano() < (files[j].LastModified.UnixNano())
	})

	if lastError != nil {
		return nil, nil, lastError
	}

	for _, file := range files {
		updateResult(snapObject, file)
	}

	return snapObject, files, nil
}

func updateResult(result map[string]any, file *SourceFile) {
	current := result
	for i, field := range file.Field {
		if i == len(file.Field)-1 { // Last element
			if file.Merge == 0 {
				current[field] = file.Value
			} else if file.Merge == 1 {
				if _, ok := current[field]; !ok {
					current[field] = make(map[string]any)
				}
				for k, v := range file.Value {
					current[field].(map[string]any)[k] = v
				}
			}
			// Assuming 'Files' and 'LastModifiedUnix' are to be appended at the last field level
			// if filesMap, ok := current[field].(map[string]any); ok {
			// 	filesMap["Files"] = append([]string{filesMap["Files"].(string)}, "oss://sample/"+file.FullPath)
			// 	filesMap["LastModifiedUnix"] = file.LastModified.Unix()
			// }
		} else {
			if _, ok := current[field]; !ok {
				current[field] = make(map[string]any)
			}
			current = current[field].(map[string]any)
		}
	}
	if len(file.Field) == 0 { // Root directory operation
		if file.Merge == 0 {
			result = file.Value
		} else if file.Merge == 1 {
			for k, v := range file.Value {
				result[k] = v
			}
		}
		for k, v := range file.Value {
			result[k] = v
		}
	}
}

func (m catalog) readOssSourceFile(file oss.ObjectProperties) (*SourceFile, error) {
	if !strings.HasSuffix(file.Key, "_0.json") && !strings.HasSuffix(file.Key, "_1.json") {
		return nil, nil
	}
	pathSplit := strings.Split(strings.Replace(file.Key, m.path+"/data/", "", 1), "/")
	var merge int
	if strings.HasSuffix(file.Key, "_0.json") {
		merge = 0
	} else if strings.HasSuffix(file.Key, "_1.json") {
		merge = 1
	} else {
		return nil, fmt.Errorf("unknown merge")
	}
	// fieldKey := ""
	// fmt.Println(file.Key, pathSplit)
	// fieldKey := pathSplit[1]
	buffer, err := m.newClient().GetObject(file.Key)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(buffer)
	if err != nil {
		return nil, err
	}
	var ret = &SourceFile{
		Prefix:       m.path,
		FullPath:     file.Key,
		FileName:     pathSplit[len(pathSplit)-1],
		Merge:        merge,
		Type:         file.Type,
		ETag:         file.ETag,
		Size:         int64(len(data)),
		LastModified: file.LastModified,
		Field:        pathSplit[:len(pathSplit)-1],
		// Data:  data,
	}
	err = json.Unmarshal(data, &ret.Value)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
