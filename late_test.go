package lake_test

import (
	_ "embed"
	"encoding/json"
	"testing"
	"time"

	"github.com/hkloudou/lake"
)

//go:embed testData/accessKeyID.secret
var accessKeyId string

//go:embed testData/accessKeySecret.secret
var accessKeySecret string

//go:embed testData/bucketName.secret
var bucketName string

func Test_Catlogs(t *testing.T) {
	c := lake.NewLake("redis://127.0.0.1:6379/2")
	t.Log(c.Catlogs())
}

func Test_Write(t *testing.T) {

	c := lake.NewLake("redis://127.0.0.1:6379/2")
	d, err := c.WiseBuild("test/91110108717743469K", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	str, _ := json.Marshal(d)
	t.Log(string(str))
	// list, err := c.List("test/91110108717743469K")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// err = c.Fetch(list)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// // t.Log(list)
	// str, _ := json.Marshal(list)
	// t.Log(string(str))
	// err := c.Write(lake.WriteDataRequest{
	// 	Unix:      time.Now().Unix(),
	// 	SeqID:     1,
	// 	RequestID: uuid.NewString(),
	// 	Prefix:    "test/91110108717743469K",
	// 	Field:     "xx.xx",
	// }, []byte(`{"qs": "value"}`))
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// client := lake.NewOssCatalog(false, "cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "test/91110108717743469K")

	// // err = c.WriteJsonData(time.Now().Unix(), 1, lake.MergeTypeOver, "s", []byte(`{"qs": "value"}`))
	// // if err != nil {
	// // 	t.Fatal(err)
	// // }
	// ti := time.Now()
	// err := client.WriteJsonData(lake.WriteDataRequest{Field: "x", Unix: ti.Unix(), SeqID: 1}, []byte(`1`))
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// err = client.WriteJsonData(lake.WriteDataRequest{Unix: ti.Unix(), SeqID: 2}, []byte(`null`))
	// if err != nil {
	// 	t.Fatal(err)
	// }

}

func TestWiseRead(t *testing.T) {
	// client := lake.NewOssCatalog(false, "cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "test/91110108717743469K")
	// result, err := client.WisebuildData(1 * time.Second)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// data, err := json.Marshal(result)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// t.Log(string(data))
}

func TestRead1(t *testing.T) {
	// client := lake.NewOssCatalog(false, "cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "test/91110108717743469K")
	// result, err := client.BuildData()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// data, err := json.Marshal(result)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// t.Log(string(data))
}

func TestLastUnix(t *testing.T) {

	// client := lake.NewOssCatalog(false, "cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "test/91110108717743469K")
	// result, err := client.ListOssFiles()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// fmt.Println(result)
	// t.Log(result.LastUnix())
}

// func TestRead(t *testing.T) {
// 	client := lake.NewOssCatalog(false, "cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "test/91110108717743469K")
// 	result, err := client.BuildData()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	// go func() {
// 	client.RemoveSnaped(result, 1*time.Minute)
// 	// }()

// 	data, err := json.Marshal(result)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	t.Log(string(data))
// 	err = client.TrySnap(result, 1*time.Minute)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }

// func Test_hdfs(t *testing.T) {
// 	oss.New("https://cs-lake-hdfs.cn-hangzhou.oss-dls.aliyuncs.com", accessKeyId, accessKeySecret)
// }
