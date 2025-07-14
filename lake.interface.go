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

func (m *lakeEngine) writeCryptOss(fullpath string, data []byte) error {
	encoded, err := encrypt(data, []byte(m.meta.AESPwd))
	if err != nil {
		return err
	}
	bucket, err := m.getBucket()
	if err != nil {
		return err
	}
	return bucket.PutObject(fullpath, bytes.NewReader(encoded))
}

func (m *lakeEngine) readCryptOSS(obj any, fullPath string) error {
	bucket, err := m.getBucket()
	if err != nil {
		return err
	}
	buffer, err := bucket.GetObject(fullPath)
	if err != nil {
		return err
	}
	data, err := io.ReadAll(buffer)
	if err != nil {
		return err
	}
	decoded, err := decrypt(data, []byte(m.meta.AESPwd))
	if err != nil {
		return err
	}
	// var tmp any
	err = json.Unmarshal(decoded, obj)
	if err != nil {
		return err
	}
	return nil
}

// func (m *lakeEngine) readCryptOSSBytes(fullPath string) ([]byte, error) {
// 	buffer, err := m.newClient().GetObject(fullPath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	data, err := io.ReadAll(buffer)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return decrypt(data, []byte(m.meta.AESPwd))
// 	// decoded, err := decrypt(data, []byte(m.meta.AESPwd))
// 	// if err != nil {
// 	// 	return nil, err
// 	// }
// 	// // var tmp any
// 	// // err = json.Unmarshal(decoded, obj)
// 	// // if err != nil {
// 	// // 	return nil, err
// 	// // }
// 	// return nil
// }

func (m *lakeEngine) writeJSON(catlog string, filePath string, data []byte) error {
	if err := m.writeCryptOss(path.Join(catlog, filePath), data); err != nil {
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
}

func (m *lakeEngine) writeSNAP(catlog string, filePath string, data []byte) error {
	if err := m.writeCryptOss(path.Join(catlog, filePath), data); err != nil {
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
		// var tmp any
		// var fetched = false
		// if len(v) > 0 {
		// 	json.Unmarshal([]byte(v), &tmp)
		// }

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
				// Value:   tmp,
				// Fetched: fetched,
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
				// Value:   tmp,
				// Fetched: fetched,
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
					var obj any
					err := m.readCryptOSS(&obj, fullPath)
					if err != nil {
						return nil, err
					}
					return obj, nil
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

// func (m *lakeEngine) Recove(filepath string) error {
// 	if err := m.readMeta(); err != nil {
// 		return err
// 	}
// 	defer m.lock.Unlock()
// 	m.lock.Lock()
// 	if strings.Trim(filepath, "/") != filepath {
// 		return fmt.Errorf("error filepath format with / prefix or suffix")
// 	}
// 	// return m.rdb.HDel(context.TODO(), m.prefix+path).Err()
// 	var data metaSnap
// 	err := m.readCryptOSS(&data, path.Join("meta", filepath))
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Println(data)
// 	// m.rdb.FlushAll(context.TODO())
// 	return nil
// }

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
	var hasIgnore bool
	snaped, err = m.trySnap(data, windows)
	if err != nil {
		return nil, err
	}
	if snaped {
		listNew := m.List(list.Catlog)
		list.Files = listNew.Files
	}

	if snaped {
		listNew := m.List(list.Catlog)
		if listNew.Err == nil && listNew.Files.HasIgnored() {
			hasIgnore = true
		}
	}
	// if list.Files.HasIgnored() {
	// 	hasIgnore = true
	// } else if snaped {
	// 	listNew := m.List(list.Catlog)
	// 	if listNew.Err == nil && listNew.Files.HasIgnored() {
	// 		hasIgnore = true
	// 	}
	// }
	if hasIgnore {
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

func (m *lakeEngine) ProdTask(therdNumer int, fn func(uuidString string, data *DataResult) error) {
	if err := m.readMeta(); err != nil {
		return
	}
	defer m.lock.Unlock()
	m.lock.Lock()

	catlogAnduuids, err := m.rdb.SRandMemberN(context.TODO(), m.keyTaskProd, 100).Result()
	if err != nil {
		fmt.Println(xcolor.Red("ProdTask.SRandMember"), err)
		return
	}
	con := threading.NewTaskRunner(therdNumer)
	for i := 0; i < len(catlogAnduuids); i++ {
		func(i int) {
			catlogAnduuid := catlogAnduuids[i]
			catlog := strings.Split(catlogAnduuid, ",")[0]
			uuidString := strings.Split(catlogAnduuid, ",")[1]
			list := m.List(catlog)
			if list.Err != nil {
				fmt.Println(xcolor.Red("ProdTask.List"), list.Err.Error())
				return
			}
			//如果不是最新的任务，则可以跳过

			if xmap.GetMapValue(list.Meta, "meta-last-uuid").String() != uuidString {
				m.rdb.SRem(context.TODO(), m.keyTaskProd, catlogAnduuid)
				return
			}
			res, err := m.Build(list)
			if err != nil {
				fmt.Println(xcolor.Red("ProdTask.Build"), err.Error())
				return
			}
			if fn(uuidString, res) == nil {
				m.rdb.SRem(context.TODO(), m.keyTaskProd, catlogAnduuid)
			}
		}(int(i) + 0)
	}
	con.Wait()
}

func (m *lakeEngine) snapMeta() error {
	if err := m.readMeta(); err != nil {
		return err
	}

	defer m.lock.Unlock()
	m.lock.Lock()
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

	taskCleanList, err := m.rdb.SMembers(context.TODO(), m.keyTaskCleanIgnore).Result()
	if err != nil {
		return err
	}
	taskProdList, err := m.rdb.SMembers(context.TODO(), m.keyTaskProd).Result()
	if err != nil {
		return err
	}
	bt, err := json.Marshal(metaSnap{
		Meta:          *m.meta,
		Datas:         results,
		TaskCleanList: taskCleanList,
		TaskProdList:  taskProdList,
	})
	if err != nil {
		return err
	}
	if err := m.writeCryptOss(fmt.Sprintf("meta/meta-%d.json", time.Now().UnixNano()), bt); err != nil {
		return err
	}
	return nil
}

func (m *lakeEngine) taskCleanignore(duration time.Duration) error {
	if err := m.readMeta(); err != nil {
		return err
	}

	catlogs, err := m.rdb.SRandMemberN(context.TODO(), m.keyTaskCleanIgnore, 100).Result()
	if err != nil {
		fmt.Println(xcolor.Red("TaskCleanignore.SRandMember"), err)
		return err
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
		if len(ossDeletingkeys) > 0 {
			bucket, err := m.getBucket()
			if err != nil {
				fmt.Println(xcolor.Red("TaskCleanignore.getBucket"), err.Error())
				continue
			}
			bucket.DeleteObjects(ossDeletingkeys)
		}
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
	return nil
}

func convertToInterface(strings []string) []interface{} {
	result := make([]interface{}, len(strings))
	for i, s := range strings {
		result[i] = s
	}
	return result
}
