package lake

import (
	"encoding/json"
	"time"
)

type fileInfo struct {
	// property oss.ObjectProperties
	Prefix string
	Path   string
	// FullPath string

	Type fileType

	Unix  int64
	Seq   int64
	Merge MergeType
	Field []string

	UUID    string
	Ignore  bool
	Fetched bool
	Value   any
}

func (o fileInfo) MarshalJSON() ([]byte, error) {
	// o.Property.
	return json.Marshal([]any{!o.Ignore, o.Type, o.Unix, o.Seq, o.Merge, o.UUID, o.Prefix, o.Path})
}

type filePropertySlice []fileInfo

func (m filePropertySlice) Len() int      { return len(m) }
func (m filePropertySlice) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m filePropertySlice) Less(i, j int) bool {
	if m[i].Unix == m[j].Unix {
		// if m[i].Type == SNAP && m[j].Type != SNAP {
		// 	return false
		// } else if m[j].Type == SNAP && m[i].Type != SNAP {
		// 	return true
		// }
		return m[i].Seq < m[j].Seq
	}
	return m[i].Unix < m[j].Unix
}

func (m filePropertySlice) LastUnix() int64 {
	for i := len(m) - 1; i >= 0; i-- {
		if !m[i].Ignore {
			return m[i].Unix
		}
	}
	return 0
}

func (m filePropertySlice) LastSnap() *fileInfo {
	for i := len(m) - 1; i >= 0; i-- {
		if m[i].Type == SNAP {
			return &m[i]
		}
	}
	return nil
}

func (m filePropertySlice) Merga() *dataResult {
	result := dataResult{
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
		updateResult(&result.Data, &m[i])
		if m[i].Unix > result.LastModifiedUnix {
			result.LastModifiedUnix = m[i].Unix
		}
	}
	result.SampleUnix = time.Now().Unix()
	result.LastSnap = m.LastSnap()
	return &result
}
