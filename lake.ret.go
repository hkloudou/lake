package lake

type listResult struct {
	Err      error
	Catlog   string
	Meta     map[string]any
	Files    filePropertySlice
	LastUnix int64
	LastSnap *fileInfo
}
