package lake

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"path"
	"sort"
	"strings"
	"time"
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

func (m *lakeEngine) List(prefix string) (filePropertySlice, error) {
	if err := m.readMeta(); err != nil {
		return nil, err
	}
	names, err := m.rdb.HKeys(context.TODO(), prefix).Result()
	if err != nil {
		return nil, err
	}
	var result = make(filePropertySlice, 0)
	for i := 0; i < len(names); i++ {
		fullName := names[i]
		fileName := path.Base(fullName)
		parts := strings.Split(strings.ReplaceAll(fileName, ".", "_"), "_")
		pathSplit := strings.Split(strings.Trim(strings.Replace(fullName, prefix, "", 1), "/"), "/")

		if strings.HasSuffix(fullName, ".snap") {
			result = append(result, fileInfo{
				Prefix: prefix,
				Path:   fullName,
				Type:   SNAP,

				Unix:  getSliceNumericPart(parts, 0),
				Seq:   getSliceNumericPart(parts, 1),
				Merge: MergeTypeOver,

				Field: nil,
			})
		} else if strings.HasSuffix(fullName, ".json") {
			result = append(result, fileInfo{
				Prefix: prefix,
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
