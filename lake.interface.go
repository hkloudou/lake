package lake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/hkloudou/xlib/threading"
	"github.com/hkloudou/xlib/xerror"
)

func (m *lakeEngine) Write(req WriteDataRequest, data []byte) error {
	if err := m.readMeta(); err != nil {
		return err
	}
	req.fix()

	if req.Merge != MergeTypeOver && req.Merge != MergeTypeUpsert {
		return fmt.Errorf("unknown merge")
	}
	if math.Abs(float64(time.Now().Unix()-req.Unix)) > req.UnixWindow.Seconds() {
		return fmt.Errorf("time is too far")
	}
	if err := m.newClient().PutObject(req.FullPath(), bytes.NewReader(data)); err != nil {
		return err
	}
	return m.rdb.HSet(context.TODO(), req.Prefix, req.Path(), "").Err()
}

func (m *lakeEngine) List(catlog string) (filePropertySlice, error) {
	if err := m.readMeta(); err != nil {
		return nil, err
	}
	names, err := m.rdb.HKeys(context.TODO(), catlog).Result()
	if err != nil {
		return nil, err
	}
	var result = make(filePropertySlice, 0)
	for i := 0; i < len(names); i++ {
		fullName := names[i]
		fileName := path.Base(fullName)
		parts := strings.Split(strings.ReplaceAll(fileName, ".", "_"), "_")
		pathSplit := strings.Split(strings.Trim(strings.Replace(fullName, catlog, "", 1), "/"), "/")

		if strings.HasSuffix(fullName, ".snap") {
			result = append(result, fileInfo{
				Prefix: catlog,
				Path:   fullName,
				Type:   SNAP,

				Unix:  getSliceNumericPart(parts, 0),
				Seq:   getSliceNumericPart(parts, 1),
				Merge: MergeTypeOver,

				Field: nil,
			})
		} else if strings.HasSuffix(fullName, ".json") {
			result = append(result, fileInfo{
				Prefix: catlog,
				Path:   fullName,
				Type:   DATA,

				Unix:  getSliceNumericPart(parts, 0),
				Seq:   getSliceNumericPart(parts, 1),
				Merge: MergeType(getSliceNumericPart(parts, 2)),

				Field: pathSplit[:len(pathSplit)-1],

				UUID: parts[3],
			})
		}
	}
	sort.Sort(result)
	lastSnap := result.LastSnap()
	if lastSnap != nil {
		for i := 0; i < len(result); i++ {
			//ignore data lte snaptime
			if result[i].Unix <= lastSnap.Unix && result[i].Path != lastSnap.Path {
				result[i].Ignore = true
			}
		}
	}
	return result, nil
}

func (m *lakeEngine) Fetch(items filePropertySlice) error {
	tasks := threading.NewTaskRunner(10)

	var be = xerror.BatchError{}
	for i := 0; i < len(items); i++ {
		if items[i].Ignore || items[i].Fetched {
			continue
		}
		func(i2 int) {
			fullPath := path.Join(items[i2].Prefix, items[i2].Path)
			tasks.Schedule(func() {
				buffer, err := m.newClient().GetObject(fullPath)
				if err != nil {
					be.Add(err)
					return
				}
				data, err := io.ReadAll(buffer)
				if err != nil {
					be.Add(err)
					return
				}
				var tmp any
				err = json.Unmarshal(data, &tmp)
				if err != nil {
					be.Add(err)
					return
				}
				items[i2].Value = tmp
				items[i2].Fetched = true
			})
		}(i)
	}
	tasks.Wait()
	return be.Err()
}

func (m *lakeEngine) Build(catlog string) (*dataResult, error) {
	items, err := m.List(catlog)
	if err != nil {
		return nil, err
	}
	err = m.Fetch(items)
	if err != nil {
		return nil, err
	}
	tmp := items.Merga()
	tmp.Catlog = catlog
	tmp.Files = items
	return tmp, nil
}

func (m *lakeEngine) WisebuildData(catlog string, windows time.Duration) (*dataResult, error) {
	data, err := m.Build(catlog)
	if err != nil {
		return nil, err
	}
	// err = m.RemoveSnaped(data, windows)
	// if err != nil {
	// 	return nil, err
	// }
	err = m.TrySnap(data, windows)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m lakeEngine) TrySnap(obj *dataResult, window time.Duration) error {
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
	data, err := json.Marshal(obj.Data)
	if err != nil {
		return err
	}

	fileName := fmt.Sprintf("%d_%d.snap", obj.LastModifiedUnix, obj.SampleUnix)
	fullPath := path.Join(obj.Catlog, fileName)

	if err := m.newClient().PutObject(
		fullPath, bytes.NewReader(data),
	); err != nil {
		return err
	}
	// fmt.Println("fileName", fileName)
	return m.rdb.HSet(context.TODO(), obj.Catlog, fileName, "").Err()
}
