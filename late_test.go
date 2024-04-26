package lake_test

import (
	_ "embed"
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
	err = c.WriteJsonData(0, "s", []byte(`{"key2": "value"}`))
	if err != nil {
		t.Fatal(err)
	}
	err = c.WriteJsonData(1, "", []byte(`{"key2": "value"}`))
	if err != nil {
		t.Fatal(err)
	}
}

func TestRead(t *testing.T) {
	c, err := lake.NewOssCatalog("cn-hangzhou", bucketName, accessKeyId, accessKeySecret, "91110108717743469K", "1_1_1100")
	if err != nil {
		t.Fatal(err)
	}
	data, err := c.BuildData(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	t.Log(data)
}
