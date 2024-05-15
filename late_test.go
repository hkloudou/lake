package lake_test

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/hkloudou/lake"
)

//go:embed testData/metaurl.txt
var metaurl string

func Test_Catlogs(t *testing.T) {
	c := lake.NewLake(metaurl)
	t.Log(c.Catlogs())
}

func Test_WiseBuild(t *testing.T) {
	c := lake.NewLake(metaurl)
	d, err := c.WiseBuild(c.List("test/91110108717743469K"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	str, _ := json.Marshal(d)
	t.Log(string(str))
}

func Test_Write(t *testing.T) {
	c := lake.NewLake(metaurl)

	err := c.Write(lake.WriteDataRequest{
		Catlog: "test/91110108717743469K",
		Field:  "xx.xx",
	}, []byte("{\"name\":\"who are you3\"}"))
	if err != nil {
		t.Fatal(err)
	}
}

func Test_List(t *testing.T) {
	c := lake.NewLake(metaurl)
	list := c.List("test/91110108717743469K")
	if list.Err != nil {
		t.Errorf("list error: %v", list.Err)
	}
	t.Log(list)
}

func Test_Prod(t *testing.T) {
	c := lake.NewLake(metaurl)
	c.ProdTask(10, func(data *lake.DataResult) error {
		// t.Log(data)

		fmt.Println("data", data)
		return nil
		return fmt.Errorf("xxx")
	})
}

func TestRead1(t *testing.T) {
	c := lake.NewLake(metaurl)
	d, err := c.Build(c.List("test/91110108717743469K"))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(d)
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
