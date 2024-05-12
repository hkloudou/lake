package lake

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type WriteDataRequest struct {
	Unix       int64
	UnixWindow time.Duration
	SeqID      int64
	Merge      MergeType
	RequestID  string
	Catlog     string
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
	if m.RequestID == "" {
		m.RequestID = strings.Split(uuid.New().String(), "-")[0]
	}
	// if m.Field == "" {
	// 	m.Field = "unknow"
	// }
}

func (m WriteDataRequest) path() string {
	arr := strings.Trim(m.Field, ".")
	fieldPath := ""
	if arr != "" {
		fieldPath = strings.ReplaceAll(arr, ".", "/") + "/"
	}

	return fmt.Sprintf("%s%d_%06d_%d_%s.json", fieldPath, m.Unix, m.SeqID, m.Merge, m.RequestID)
}

func (m WriteDataRequest) FullPath() string {
	return fmt.Sprintf("%s/%s", m.Catlog, m.path())
}
