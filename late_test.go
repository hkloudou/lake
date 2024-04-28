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

func TestXxx(t *testing.T) {

	client := lake.NewOssCatalog(false, "cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "test/91110108717743469K")

	// err = c.WriteJsonData(time.Now().Unix(), 1, lake.MergeTypeOver, "s", []byte(`{"qs": "value"}`))
	// if err != nil {
	// 	t.Fatal(err)
	// }
	err := client.WriteJsonData(time.Now().Unix(), 2, lake.MergeTypeOver, "as", []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
}

func TestRead(t *testing.T) {
	client := lake.NewOssCatalog(false, "cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "test/91110108717743469K")

	result, err := client.BuildData()
	if err != nil {
		t.Fatal(err)
	}
	// go func() {
	client.RemoveSnaped(result, 1*time.Minute)
	// }()

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
	err = client.TrySnap(result, 1*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
}
