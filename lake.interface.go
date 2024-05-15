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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hkloudou/xlib/threading"
	"github.com/hkloudou/xlib/xcolor"
	"github.com/hkloudou/xlib/xerror"
	"github.com/hkloudou/xlib/xmap"
)

func (m *lakeEngine) writeJSON(catlog string, filePath string, data []byte) error {
	if err := m.newClient().PutObject(path.Join(catlog, filePath), bytes.NewReader(data)); err != nil {
		return err
	}
	uuidString := uuid.NewString()
	// m.rdb
	luaScript := `
	local catlog = KEYS[1]
	local filePath = ARGV[1]
	local uuidString = ARGV[2]
	local prefix = ARGV[3]
	local keyTask = ARGV[4]

	-- 设置哈希表
	redis.call("HSET", prefix .. catlog, filePath, "")
	redis.call("HSET", prefix .. catlog, "meta-last-uuid", '"' .. uuidString .. '"')

	-- 添加到集合
	return redis.call("SADD", keyTask, catlog .. "," .. uuidString)
	`
	_, err := m.rdb.Eval(context.TODO(), luaScript, []string{catlog}, filePath, uuidString, m.prefix, m.keyTaskProd).Result()
	return err
	// m.rdb.
	// err := m.rdb.HSet(context.TODO(), m.prefix+catlog, []string{
	// 	filePath, "",
	// 	"meta-last-uuid", fmt.Sprintf(`"%s"`, uuidString),
	// }).Err()
	// if err != nil {
	// 	return err
	// }
	// return m.rdb.SAdd(context.TODO(), m.keyTask, fmt.Errorf(`%s,%s`, catlog, uuidString)).Err()
}

func (m *lakeEngine) writeSNAP(catlog string, filePath string, data []byte) error {
	if err := m.newClient().PutObject(path.Join(catlog, filePath), bytes.NewReader(data)); err != nil {
		return err
	}
	return m.rdb.HSet(context.TODO(), m.prefix+catlog, []string{
		filePath, "",
	}).Err()
}

func (m *lakeEngine) Write(req WriteDataRequest, data []byte) error {
	if err := m.readMeta(); err != nil {
		return err
	}
	req.fix()
	if strings.Trim(req.Catlog, "/") != req.Catlog {
		return fmt.Errorf("error catlog format with / prefix or suffix")
	}

	if req.Merge != MergeTypeOver && req.Merge != MergeTypeUpsert {
		return fmt.Errorf("unknown merge")
	}
	if math.Abs(float64(time.Now().Unix()-req.Unix)) > req.UnixWindow.Seconds() {
		return fmt.Errorf("time is too far")
	}
	return m.writeJSON(req.Catlog, req.path(), data)
	// if err := m.newClient().PutObject(req.FullPath(), bytes.NewReader(data)); err != nil {
	// 	return err
	// }
	// return m.rdb.HSet(context.TODO(), m.prefix+req.Catlog, req.path(), "").Err()
}

func (m *lakeEngine) Catlogs() ([]string, error) {
	if err := m.readMeta(); err != nil {
		return nil, err
	}
	// fmt.Println("pre", m.prefix+"*")
	keys, err := m.rdb.Keys(context.TODO(), m.prefix+"*").Result()
	if err != nil {
		return nil, err
	}
	keysStart := len(m.prefix)
	for i := 0; i < len(keys); i++ {
		keys[i] = keys[i][keysStart:]
	}
	return keys, nil
}

func (m *lakeEngine) List(catlog string) *listResult {
	var result = listResult{
		Catlog: catlog,
		Meta:   make(map[string]any),
		Files:  make(filePropertySlice, 0),
	}
	if err := m.readMeta(); err != nil {
		result.Err = err
		return &result
	}
	if strings.Trim(catlog, "/") != catlog {
		result.Err = fmt.Errorf("error catlog format with / prefix or suffix")
		return &result
	}

	names, err := m.rdb.HGetAll(context.TODO(), m.prefix+catlog).Result()
	if err != nil {
		result.Err = err
		return &result
	}
	// var result = make(filePropertySlice, 0)
	for k, v := range names {
		if strings.HasPrefix(k, "meta-") {
			var tmp any
			json.Unmarshal([]byte(v), &tmp)
			result.Meta[k] = tmp
			continue
		}
		fullName := k
		fileName := path.Base(fullName)
		parts := strings.Split(strings.ReplaceAll(fileName, ".", "_"), "_")
		pathSplit := strings.Split(strings.Trim(strings.Replace(fullName, catlog, "", 1), "/"), "/")

		if strings.HasSuffix(fullName, ".snap") {
			result.Files = append(result.Files, fileInfo{
				Prefix: catlog,
				Path:   fullName,
				Type:   SNAP,

				Unix:  getSliceNumericPart(parts, 0),
				Seq:   getSliceNumericPart(parts, 1),
				Merge: MergeTypeOver,

				Field: nil,
			})
		} else if strings.HasSuffix(fullName, ".json") {
			result.Files = append(result.Files, fileInfo{
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
	sort.Sort(result.Files)
	result.LastSnap = result.Files._lastSnap()
	result.LastUnix = result.Files._lastUnix()
	if result.LastSnap != nil {
		for i := 0; i < len(result.Files); i++ {
			//ignore data lte snaptime
			if result.Files[i].Unix <= result.LastSnap.Unix && result.Files[i].Path != result.LastSnap.Path {
				result.Files[i].Ignore = true
			}
		}
	}
	return &result
}

func (m *lakeEngine) fetch(items filePropertySlice) error {
	tasks := threading.NewTaskRunner(10)

	// m.cache.Take(items)

	var be = xerror.BatchError{}
	for i := 0; i < len(items); i++ {
		if items[i].Ignore || items[i].Fetched {
			continue
		}

		func(i int) {
			tasks.Schedule(func() {
				fullPath := path.Join(items[i].Prefix, items[i].Path)
				// m.cache.
				// fmt.Println()
				// _, found := m.cache.Get(fullPath)
				// if !found {
				// 	fmt.Println(xcolor.Red("not found"), fullPath)
				// }
				tmp, err := m.cache.Take(fullPath, func() (any, error) {
					// fmt.Println(xcolor.Yellow("fetch"), fullPath)
					buffer, err := m.newClient().GetObject(fullPath)
					if err != nil {
						return nil, err
					}
					data, err := io.ReadAll(buffer)
					if err != nil {
						return nil, err
					}
					var tmp any
					err = json.Unmarshal(data, &tmp)
					if err != nil {
						return nil, err
					}
					return tmp, nil
				})
				if err != nil {
					be.Add(err)
					return
				}
				items[i].Value = tmp
				items[i].Fetched = true
			})
		}(int(i))
	}
	tasks.Wait()
	return be.Err()
}

func (m *lakeEngine) Build(list *listResult) (*DataResult, error) {
	if list.Err != nil {
		return nil, list.Err
	}
	if err := m.readMeta(); err != nil {
		return nil, err
	}
	if strings.Trim(list.Catlog, "/") != list.Catlog {
		return nil, fmt.Errorf("error catlog format with / prefix or suffix")
	}

	err := m.fetch(list.Files)
	if err != nil {
		return nil, err
	}
	tmp := list.Files.Merga()
	tmp.Catlog = list.Catlog
	tmp.Files = list.Files
	return tmp, nil
}

func (m *lakeEngine) WiseBuild(list *listResult, windows time.Duration) (*DataResult, error) {
	if list.Err != nil {
		return nil, list.Err
	}
	if err := m.readMeta(); err != nil {
		return nil, err
	}
	if strings.Trim(list.Catlog, "/") != list.Catlog {
		return nil, fmt.Errorf("error catlog format with / prefix or suffix")
	}

	data, err := m.Build(list)
	if err != nil {
		return nil, err
	}
	var snaped bool
	snaped, err = m.trySnap(data, windows)
	if err != nil {
		return nil, err
	}
	if snaped {
		listNew := m.List(list.Catlog)
		list.Files = listNew.Files
	}

	if list.Files.HasIgnored() {
		m.rdb.SAdd(context.TODO(), m.keyTaskCleanIgnore, list.Catlog)
	}

	return data, nil
}

func (m *lakeEngine) trySnap(obj *DataResult, window time.Duration) (bool, error) {
	if err := m.readMeta(); err != nil {
		return false, err
	}
	if !obj.ShouldSnap(window) {
		return false, nil
	}
	if obj.LastModifiedUnix == 0 || obj.SampleUnix == 0 {
		return false, nil
	}
	if obj.SampleUnix-obj.LastModifiedUnix < int64(window.Seconds()) {
		return false, fmt.Errorf("too short time")
	}
	data, err := json.Marshal(obj.Data)
	if err != nil {
		return false, err
	}

	return true, m.writeSNAP(obj.Catlog, fmt.Sprintf("%d_%d.snap", obj.LastModifiedUnix, obj.SampleUnix), data)
}

func (m *lakeEngine) ProdTask(num int64, fn func(uuidString string, data *DataResult) error) {
	if err := m.readMeta(); err != nil {
		return
	}
	catlogAnduuids, err := m.rdb.SRandMemberN(context.TODO(), m.keyTaskProd, num).Result()
	if err != nil {
		fmt.Println(xcolor.Red("ProdTask.SRandMember"), err)
		return
	}
	for i := 0; i < len(catlogAnduuids); i++ {
		catlogAnduuid := catlogAnduuids[i]
		catlog := strings.Split(catlogAnduuid, ",")[0]
		uuidString := strings.Split(catlogAnduuid, ",")[1]
		list := m.List(catlog)
		if list.Err != nil {
			fmt.Println(xcolor.Red("ProdTask.List"), list.Err.Error())
			continue
		}
		//如果不是最新的任务，则可以跳过

		if xmap.GetMapValue(list.Meta, "meta-last-uuid").String() != uuidString {
			m.rdb.SRem(context.TODO(), m.keyTaskProd, catlogAnduuid)
			continue
		}
		res, err := m.Build(list)
		if err != nil {
			fmt.Println(xcolor.Red("ProdTask.Build"), err.Error())
			continue
		}
		if fn(uuidString, res) == nil {
			m.rdb.SRem(context.TODO(), m.keyTaskProd, catlogAnduuid)
		}
	}
}

func (m *lakeEngine) SnapMetaLoop(duration time.Duration) {
	if err := m.readMeta(); err != nil {
		return
	}
	m.snapMetaTasker.Do(func() {
		go func() {
			for {
				err := m.SnapMeta()
				if err != nil {
					fmt.Println(xcolor.Red("SnapMeta"), err.Error())
				}
				time.Sleep(duration)
			}
		}()
	})
}

func (m *lakeEngine) SnapMeta() error {
	if err := m.readMeta(); err != nil {
		return err
	}
	// m.List("").Files.Merga()
	catlogs, err := m.Catlogs()
	if err != nil {
		return err
	}
	be := xerror.BatchError{}
	var results = make([]listResult, 0)
	lock := sync.Mutex{}
	cond := threading.NewTaskRunner(50)
	for i := 0; i < len(catlogs); i++ {
		func(catlog string) {
			res := m.List(catlog)
			if res.Err != nil {
				be.Add(res.Err)
				return
			}
			lock.Lock()
			results = append(results, *res)
			lock.Unlock()
		}(catlogs[i])
	}
	cond.Wait()
	if be.Err() != nil {
		return be.Err()
	}
	bt, err := json.Marshal(results)
	if err != nil {
		return err
	}
	if err := m.newClient().PutObject(fmt.Sprintf("meta/meta-%d.json", time.Now().UnixNano()), bytes.NewReader(bt)); err != nil {
		return err
	}
	return nil
}

func (m *lakeEngine) TaskCleanignore(num int64, duration time.Duration) {
	if err := m.readMeta(); err != nil {
		return
	}
	catlogs, err := m.rdb.SRandMemberN(context.TODO(), m.keyTaskCleanIgnore, num).Result()
	if err != nil {
		fmt.Println(xcolor.Red("TaskCleanignore.SRandMember"), err)
		return
	}
	for i := 0; i < len(catlogs); i++ {
		list := m.List(catlogs[i])
		if list.Err != nil {
			continue
		}
		ossDeletingkeys := make([]string, 0)
		redisDeletKeys := make([]string, 0)
		for _, file := range list.Files {
			if file.Ignore && (time.Now().Unix()-file.Unix > int64(duration.Seconds())) {
				ossDeletingkeys = append(ossDeletingkeys, path.Join(file.Prefix, file.Path))
				redisDeletKeys = append(redisDeletKeys, file.Path)
			}
		}
		m.newClient().DeleteObjects(ossDeletingkeys)
		luaScript := `
    local prefix = ARGV[1]
    local catlog = ARGV[2]
    local keyTaskCleanIgnore = ARGV[3]
    local redisDeletKeys = {}

    for i = 4, #ARGV do
        table.insert(redisDeletKeys, ARGV[i])
    end

    if #redisDeletKeys > 0 then
        redis.call("HDEL", prefix .. catlog, unpack(redisDeletKeys))
    end

    redis.call("SREM", keyTaskCleanIgnore, catlog)
    `
		args := append([]interface{}{m.prefix, catlogs[i], m.keyTaskCleanIgnore}, convertToInterface(redisDeletKeys)...)
		_, err := m.rdb.Eval(context.TODO(), luaScript, nil, args...).Result()
		if err != nil {
			fmt.Println(xcolor.Red("TaskCleanignore.Eval"), err.Error())
		}
	}
}

func convertToInterface(strings []string) []interface{} {
	result := make([]interface{}, len(strings))
	for i, s := range strings {
		result[i] = s
	}
	return result
}
