package lake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

func (m catalog) WriteJsonData(timeUnix int64, seqid int64, merge MergeType, field string, data []byte) error {
	if merge != 0 && merge != 1 {
		return fmt.Errorf("unknown merge")
	}
	if math.Abs(float64(time.Now().Unix()-timeUnix)) > 60 {
		return fmt.Errorf("time is too far")
	}
	arr := strings.Trim(field, ".")
	fieldPath := ""
	if arr != "" {
		fieldPath = strings.ReplaceAll(arr, ".", "/") + "/"
	}
	seqid = seqid & 0xFFFF //0-65535
	//${unix}_${%06d:seq_id}_${uuid}_${merge}.${format}
	return m.newClient().PutObject(fmt.Sprintf("%s/%s%d_%06d_%d_%s.json", m.path, fieldPath, timeUnix, seqid, merge, strings.Split(uuid.New().String(), "-")[0]), bytes.NewReader(data))
}

func (m catalog) WriteSnap(obj *ossDataResult, window time.Duration) error {
	if obj.LastModifiedUnix == 0 || obj.SampleUnix == 0 {
		return nil
	}
	if time.Now().Unix()-obj.SampleUnix < int64(window.Seconds()) {
		return fmt.Errorf("too short time")
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	//need't snap

	// arr := strings.Trim(field, ".")
	// fieldPath := ""
	// if arr != "" {
	// 	fieldPath = strings.ReplaceAll(arr, ".", "/") + "/"
	// }
	err = m.newClient().PutObject(
		fmt.Sprintf("%s/snap/%d.snap", m.path, obj.SampleUnix), bytes.NewReader(data),
	)
	if err != nil {
		return err
	}
	/*
		尝试归档
	*/
	return nil
	// 设置对象的HTTP头部
	// options := []oss.Option{
	// 	oss.Meta("Last-Modified", time.Unix(obj.LastModifiedUnix, 0).Format(time.RFC1123)),
	// }
}

func (m catalog) BuildData(sampleUnix int64) (*ossDataResult, error) {
	//list information
	items, err := m.ListOssFiles(sampleUnix)
	if err != nil {
		return nil, err
	}
	err = items.Fetch(&m)
	if err != nil {
		return nil, err
	}
	return items.Merga(sampleUnix), nil
}

func updateResult(result map[string]any, file *ossFileProperty) {
	current := result
	for i, field := range file.Field {
		if i == len(file.Field)-1 { // Last element
			if file.Merge == MergeTypeOver {
				current[field] = file.Value
			} else if file.Merge == MergeTypeUpsert {
				if _, ok := current[field]; !ok {
					current[field] = make(map[string]any)
				}
				for k, v := range file.Value.(map[string]any) {
					current[field].(map[string]any)[k] = v
				}
			}
		} else {
			if _, ok := current[field]; !ok {
				current[field] = make(map[string]any)
			}
			current = current[field].(map[string]any)
		}
	}
	if len(file.Field) == 0 { // Root directory operation
		if file.Merge == MergeTypeOver {
			result = file.Value.(map[string]any)
		} else if file.Merge == MergeTypeUpsert {
			for k, v := range file.Value.(map[string]any) {
				result[k] = v
			}
		}
		for k, v := range file.Value.(map[string]any) {
			result[k] = v
		}
	}
}

// func (m catalog) readOssSourceFile(file oss.ObjectProperties) (*SourceFile, error) {
// 	if !strings.HasSuffix(file.Key, "_0.json") && !strings.HasSuffix(file.Key, "_1.json") {
// 		return nil, nil
// 	}
// 	pathSplit := strings.Split(strings.Replace(file.Key, m.path+"/data/", "", 1), "/")
// 	var merge int
// 	if strings.HasSuffix(file.Key, "_0.json") {
// 		merge = 0
// 	} else if strings.HasSuffix(file.Key, "_1.json") {
// 		merge = 1
// 	} else {
// 		return nil, fmt.Errorf("unknown merge")
// 	}
// 	// fieldKey := ""
// 	// fmt.Println(file.Key, pathSplit)
// 	// fieldKey := pathSplit[1]
// 	buffer, err := m.newClient().GetObject(file.Key)
// 	if err != nil {
// 		return nil, err
// 	}
// 	data, err := io.ReadAll(buffer)
// 	if err != nil {
// 		return nil, err
// 	}
// 	var ret = &SourceFile{
// 		Prefix:       m.path,
// 		FullPath:     file.Key,
// 		FileName:     pathSplit[len(pathSplit)-1],
// 		Merge:        merge,
// 		Type:         file.Type,
// 		ETag:         file.ETag,
// 		Size:         int64(len(data)),
// 		LastModified: file.LastModified,
// 		Field:        pathSplit[:len(pathSplit)-1],
// 		// Data:  data,
// 	}
// 	err = json.Unmarshal(data, &ret.Value)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return ret, nil
// }
