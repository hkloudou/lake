package lake

type StorageClass string
type TextFormat string
type MergeType uint8

const (
	StorageClassOSS  StorageClass = "OSS"
	StorageClassFile StorageClass = "FILE"
)

const (
	TextFormatSNAP TextFormat = "SNAP"
	TextFormatJSON TextFormat = "JSON"
)

const (
	MergeTypeOver   MergeType = 1
	MergeTypeUpsert MergeType = 2
)
