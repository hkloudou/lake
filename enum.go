package lake

type StorageClass string
type TextFormat string

const (
	StorageClassOSS  StorageClass = "OSS"
	StorageClassFile StorageClass = "FILE"
)

const (
	TextFormatSNAP TextFormat = "SNAP"
	TextFormatJSON TextFormat = "JSON"
)
