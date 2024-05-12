package lake

import "time"

type dataResult struct {
	Catlog           string            `json:"catlog"`
	Data             map[string]any    `json:"data"`
	Files            filePropertySlice `json:"-"` // [ignore, format, unix, seqid, merge, uuid, key
	LastModifiedUnix int64             `json:"lastModifiedUnix"`
	SampleUnix       int64             `json:"sampleUnix"`
	LastSnap         *fileInfo         `json:"-"`
}

func (o dataResult) ShouldSnap(window time.Duration) bool {
	return o.SampleUnix-o.LastModifiedUnix > int64(window.Seconds()) && (o.LastSnap == nil || o.LastSnap.Unix != o.LastModifiedUnix)
}
