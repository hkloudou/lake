package lake_test

import (
	_ "embed"
	"encoding/json"
	"fmt"
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

func TestXxx(t *testing.T) {

	c, err := lake.NewOssCatalog(false, "cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "1/1/91110108717743469K")
	if err != nil {
		t.Fatal(err)
	}
	// err = c.WriteJsonData(time.Now().Unix(), 1, lake.MergeTypeOver, "s", []byte(`{"qs": "value"}`))
	// if err != nil {
	// 	t.Fatal(err)
	// }
	err = c.WriteJsonData(time.Now().Unix(), 2, lake.MergeTypeOver, "as", []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
}

func TestRead(t *testing.T) {
	c, err := lake.NewOssCatalog(false, "cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "1/1/91110108717743469K")
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.BuildData(0)
	// result, sampleUnix, err := c.BuildData(0)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(result)
	fmt.Println(string(b))
	result.Files.RemoveOld(c)
	// for i := 0; i < len(result.Files); i++ {
	// 	fmt.Println(result.Files[i][0], "\t", result.Files[i][1], "\t", result.Files[i][2])
	// }
	// t.Log(result.LastModifiedUnix, sampleUnix, result.Data)
	err = c.WriteSnap(result, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
}
