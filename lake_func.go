package lake

import (
	"context"
	"encoding/json"
	"unsafe"

	"github.com/tidwall/gjson"
)

func ReadBytes(ctx context.Context, list *ListResult) ([]byte, error) {
	return list.client.readData(ctx, list)
}

func ReadString(ctx context.Context, list *ListResult) (string, error) {
	data, err := list.client.readData(ctx, list)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func ReadMap(ctx context.Context, list *ListResult) (map[string]any, error) {
	data, err := list.client.readData(ctx, list)
	if err != nil {
		return nil, err
	}

	result, err := parseJSON[map[string]any](data)
	if err != nil {
		return nil, err
	}

	return *result, nil
}

func Read[T any](ctx context.Context, list *ListResult) (*T, error) {
	data, err := list.client.readData(ctx, list)
	if err != nil {
		return nil, err
	}

	return parseJSON[T](data)
}

func parseJSON[T any](data []byte) (*T, error) {
	var result T

	if _, ok := any(result).([]byte); ok {
		return (*T)(unsafe.Pointer(&data)), nil
	}
	if _, ok := any(result).(string); ok {
		tmp := string(data)
		return (*T)(unsafe.Pointer(&tmp)), nil
	}

	// 特殊处理 gjson.Result 类型
	if _, ok := any(result).(gjson.Result); ok {
		gjsonResult := gjson.ParseBytes(data)
		// 使用 any 转换避免类型断言问题
		if ptr, ok := any(&gjsonResult).(*T); ok {
			return ptr, nil
		}
		// 这种情况理论上不会发生，但为了安全还是处理一下
		return nil, nil
	}

	// 标准 JSON 解析
	err := json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
