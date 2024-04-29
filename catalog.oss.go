package lake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/google/uuid"
)

type WriteDataRequest struct {
	Unix       int64
	UnixWindow time.Duration
	SeqID      int64
	Merge      MergeType
	RequestID  string
	Field      string
}

func (m *WriteDataRequest) fix() {
	if m.Unix == 0 {
		m.Unix = time.Now().Unix()
	}
	if m.UnixWindow == 0 {
		m.UnixWindow = 60 * time.Second
	}
	if m.Merge == 0 {
		m.Merge = MergeTypeOver
	}
	if m.Field == "" {
		m.Field = "unknow"
	}
}

func (m WriteDataRequest) Path(prefixPath string) string {
	arr := strings.Trim(m.Field, ".")
	fieldPath := ""
	if arr != "" {
		fieldPath = strings.ReplaceAll(arr, ".", "/") + "/"
	}
	// req.SeqID //= seqid & 0xFFFF //0-65535
	//${unix}_${%06d:seq_id}_${uuid}_${merge}.${format}\
	requestID := m.RequestID
	if requestID == "" {
		requestID = strings.Split(uuid.New().String(), "-")[0]
	}

	// fmt.p
	return fmt.Sprintf("%s/%s%d_%06d_%d_%s.json", prefixPath, fieldPath, m.Unix, m.SeqID, m.Merge, requestID)
}

func (m catalog) WriteJsonData(req WriteDataRequest, data []byte) error {
	req.fix()
	if req.Merge != MergeTypeOver && req.Merge != MergeTypeUpsert {
		return fmt.Errorf("unknown merge")
	}
	if math.Abs(float64(time.Now().Unix()-req.Unix)) > req.UnixWindow.Seconds() {
		return fmt.Errorf("time is too far")
	}

	return m.newClient().PutObject(req.Path(m.path), bytes.NewReader(data))
}

func (m catalog) TrySnap(obj *ossDataResult, window time.Duration) error {
	if !obj.ShouldSnap(window) {
		return nil
	}
	if obj.LastModifiedUnix == 0 || obj.SampleUnix == 0 {
		return nil
	}
	if obj.SampleUnix-obj.LastModifiedUnix < int64(window.Seconds()) {
		return fmt.Errorf("too short time")
	}
	// fmt.Println("snap")
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	err = m.newClient().PutObject(
		fmt.Sprintf("%s/%d_%d.snap", m.path, obj.LastModifiedUnix, obj.SampleUnix), bytes.NewReader(data),
	)
	if err != nil {
		return err
	}
	return nil
}

func (m catalog) TagSnaped(obj *ossDataResult) {
	// obj * ossDataResult
	if obj.LastSnap == nil {
		return
	}
	for i := 0; i < len(obj.Files); i++ {
		if obj.Files[i].Ignore && obj.LastSnap.Fetched &&
			obj.Files[i].Unix <= obj.LastSnap.Unix && obj.Files[i].Property.Key != obj.LastSnap.Property.Key {
			fmt.Println("tag", obj.Files[i].Property.Key)
			m.newClient().PutObjectTagging(obj.Files[i].Property.Key, oss.Tagging{
				Tags: []oss.Tag{
					{Key: "hkloudou.lake-deleting", Value: "true"},
				},
			})
		}
	}
}

func (m catalog) RemoveSnaped(obj *ossDataResult, windows time.Duration) error {
	if obj.LastSnap == nil {
		return nil
	}
	// fmt.Println(time.Now().Unix(), obj.SampleUnix, obj.LastSnap.SeqID, windows.Seconds())
	//make sure snap file is not too new
	if obj.SampleUnix-obj.LastSnap.SeqID < int64(windows.Seconds()) {
		return nil
	}
	var deleteList = make([]string, 0)
	for i := 0; i < len(obj.Files); i++ {
		if obj.Files[i].Ignore &&
			obj.Files[i].Unix <= obj.LastSnap.Unix &&
			obj.Files[i].Property.Key != obj.LastSnap.Property.Key {
			deleteList = append(deleteList, obj.Files[i].Property.Key)
		}
	}
	if len(deleteList) > 0 {
		fmt.Println("deleteList", deleteList)
		_, err := m.newClient().DeleteObjects(deleteList)
		return err
	}
	return nil
}

func (m catalog) BuildData() (*ossDataResult, error) {
	//list information
	items, err := m.ListOssFiles()
	if err != nil {
		return nil, err
	}
	err = items.Fetch(&m)
	if err != nil {
		return nil, err
	}
	return items.Merga(), nil
}

func (m catalog) WisebuildData(windows time.Duration) (*ossDataResult, error) {
	data, err := m.BuildData()
	if err != nil {
		return nil, err
	}
	err = m.RemoveSnaped(data, windows)
	if err != nil {
		return nil, err
	}
	err = m.TrySnap(data, windows)
	if err != nil {
		return nil, err
	}
	return data, nil
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
