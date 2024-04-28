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

	c, err := lake.NewOssCatalog(false, "cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "test/91110108717743469K")
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
	c, err := lake.NewOssCatalog(false, "cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "test/91110108717743469K")
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.BuildData()
	if err != nil {
		t.Fatal(err)
	}
	// go func() {
	c.RemoveSnaped(result)
	// }()

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
	if result.ShouldSnap(1 * time.Minute) {
		fmt.Println("snap")
		err = c.WriteSnap(result, 1*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		//remove old
	}
}
