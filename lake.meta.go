package lake

import "encoding/json"

type Meta struct {
	Name string
	// UUID      string
	Storage   string
	Bucket    string
	location  string `json:"-"`
	AccessKey string
	SecretKey string //encoded
	AESPwd    string
}

func (m *Meta) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

type metaSnap struct {
	Meta          Meta
	TaskCleanList []string
	TaskProdList  []string
	Datas         []listResult
}
