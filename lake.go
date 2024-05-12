package lake

type fileType uint8

const (
	DATA fileType = 1
	SNAP fileType = 2
)

type LakeProvider interface {
	WriteJson()
	List() ([]filePropertySlice, error)
	Fetch([]filePropertySlice) error
}
