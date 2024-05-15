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

	d, err := c.WiseBuild(c.List("test/91110108717743469K"), 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(d.Files)
	str, _ := json.Marshal(d)
	t.Log(string(str))
}

func Test_Write(t *testing.T) {
	c := lake.NewLake(metaurl)
	err := c.Write(lake.WriteDataRequest{
		Catlog: "test/91110108717743469K",
		Field:  "cfg.area",
	}, []byte("4100"))
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
	c.ProdTask(10, func(uuidString string, data *lake.DataResult) error {
		// t.Log(data)
		fmt.Println(uuidString, "data", data)
		return nil
		return fmt.Errorf("xxx")
	})
}

func Test_Clean(t *testing.T) {
	// c := lake.NewLake(metaurl)

	// c.(10, 1*time.Second)
}

func TestRead1(t *testing.T) {
	c := lake.NewLake(metaurl)
	d, err := c.Build(c.List("test/91110108717743469K"))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(d)
}

func TestRecover(t *testing.T) {
	// c := lake.NewLake(metaurl, lake.WithMetaSnapTTL(10*time.Minute))
	// err := c.Recove("meta-1715753906274156000.json")
	// if err != nil {
	// 	t.Error(err)
	// }
}

func Test_ProdTask(t *testing.T) {
	c := lake.NewLake(metaurl)
	c.ProdTask(1, func(uuidString string, data *lake.DataResult) error {
		return nil
	})
	// c(1 * time.Hour)
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
