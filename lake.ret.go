package lake

type listResult struct {
	Err      error `json:"-"`
	Catlog   string
	Meta     map[string]any
	Files    filePropertySlice
	LastUnix int64     `json:"-"`
	LastSnap *fileInfo `json:"-"`
}
