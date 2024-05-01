package main

import (
	"os"

	"github.com/aliyun/fc-runtime-go-sdk/fc"
	"github.com/hkloudou/lake"
	"rogchap.com/v8go"
)

type Event struct {
	Keys      []string `json:"keys"`
	MapScript string   `json:"map"`
}

func main() {
	fc.Start(HandleRequest)
}

func HandleRequest(event Event) ([]any, error) {
	var results []any
	for i := 0; i < len(event.Keys); i++ {
		client := lake.NewOssCatalog(os.Getenv("FC_REGION") == "cn-hangzhou", "cn-hangzhou", os.Getenv("OSS_BUCKET_NAME"), os.Getenv("OSS_ACCESS_KEY_ID"), os.Getenv("OSS_ACCESS_KEY_SECRET"), event.Keys[i])
		res, err := client.BuildData()
		if err != nil {
			return nil, err
		}
		ctx := v8go.NewContext()                           // new context with a default VM
		obj := ctx.Global()                                // get the global object from the context
		obj.Set("res", res)                                // set the property "version" on the object
		val, _ := ctx.RunScript(event.MapScript, "map.js") // global object will have the property set within the JS VM
		defer val.Release()
		results = append(results, val)
	}
	return results, nil
}
