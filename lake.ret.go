package lake

type listMeta map[string]any

func (m listMeta) GetInt(key string) int {
	return m.GetIntDefault(key, 0)
}
func (m listMeta) GetIntDefault(key string, value int) int {
	return GetMapInt(m, key, value)
}

func (m listMeta) GetString(key string) string {
	return m.GetStringDefault(key, "")
}

func (m listMeta) GetStringDefault(key string, value string) string {
	return GetMapString(m, key, value)
}

type listResult struct {
	Meta     listMeta
	Files    filePropertySlice
	LastUnix int64
	LastSnap *fileInfo
}

// func (m fileList) Merga() *dataResult {
// 	result := dataResult{
// 		Data:             map[string]any{},
// 		Files:            m.Files,
// 		LastModifiedUnix: 0,
// 		// SampleUnix:       sampleUnix,
// 	}
// 	for i := 0; i < len(m.Files); i++ {
// 		// fmt.Println(!m[i].Ignore, m[i].Value)
// 		if m.Files[i].Ignore {
// 			continue
// 		}
// 		updateResult(&result.Data, &m.Files[i])
// 		if m.Files[i].Unix > result.LastModifiedUnix {
// 			result.LastModifiedUnix = m.Files[i].Unix
// 		}
// 	}
// 	result.SampleUnix = time.Now().Unix()
// 	result.LastSnap = m.LastSnap
// 	return &result
// }
