package lake_test

import (
	_ "embed"
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

	c, err := lake.NewOssCatalog("cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "91110108717743469K", "1_1_1100")
	if err != nil {
		t.Fatal(err)
	}
	err = c.WriteJsonData(0, 1001, "s", []byte(`{"qs": "value"}`))
	if err != nil {
		t.Fatal(err)
	}
	// err = c.WriteJsonData(1, 1001, "s", []byte(`{"aas": "value"}`))
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// err = c.WriteJsonData(1, 123, "", []byte(`{"key2": "value"}`))
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// err = c.WriteJsonData(1, 24, "", []byte(`{"key2": "value"}`))
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// err = c.WriteJsonData(0, 2, "s", []byte(`{"key2": "value"}`))
	// if err != nil {
	// 	t.Fatal(err)
	// }
}

func TestRead(t *testing.T) {
	c, err := lake.NewOssCatalog("cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "91110108717743469K", "1_1_1100")
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.BuildData(time.Now().Unix() - 60)
	if err != nil {
		t.Fatal(err)
	}
	// t.Log()
	for i := 0; i < len(result.Files); i++ {
		fmt.Println(result.Files[i][0], "\t", result.Files[i][1], "\t", result.Files[i][2])
	}
	t.Log(result.LastModifiedUnix, result.Data)
	// c.WriteSnap(result)
}
