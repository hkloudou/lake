package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tidwall/gjson"
)

// ReadBytes returns the merged document as raw bytes.
func ReadBytes(ctx context.Context, list *ListResult) ([]byte, error) {
	if list == nil || list.client == nil {
		return nil, errors.New("lake: Read requires a ListResult from List/BatchList")
	}
	return list.client.readData(ctx, list)
}

// ReadString returns the merged document as a JSON string.
func ReadString(ctx context.Context, list *ListResult) (string, error) {
	if list == nil || list.client == nil {
		return "", errors.New("lake: Read requires a ListResult from List/BatchList")
	}
	data, err := list.client.readData(ctx, list)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ReadMap returns the merged document parsed as map[string]any.
func ReadMap(ctx context.Context, list *ListResult) (map[string]any, error) {
	if list == nil || list.client == nil {
		return nil, errors.New("lake: Read requires a ListResult from List/BatchList")
	}
	data, err := list.client.readData(ctx, list)
	if err != nil {
		return nil, err
	}
	out, err := parseJSON[map[string]any](data)
	if err != nil {
		return nil, err
	}
	return *out, nil
}

// Read returns the merged document parsed into a value of type T.
//
// Special-cased: T == []byte / string / gjson.Result skip JSON unmarshal
// and wrap the raw bytes directly.
func Read[T any](ctx context.Context, list *ListResult) (*T, error) {
	if list == nil || list.client == nil {
		return nil, errors.New("lake: Read requires a ListResult from List/BatchList")
	}
	data, err := list.client.readData(ctx, list)
	if err != nil {
		return nil, err
	}
	return parseJSON[T](data)
}

func parseJSON[T any](data []byte) (*T, error) {
	var zero T
	switch any(zero).(type) {
	case []byte:
		v, ok := any(data).(T)
		if !ok {
			return nil, fmt.Errorf("parseJSON: cannot coerce []byte to %T", zero)
		}
		return &v, nil
	case string:
		v, ok := any(string(data)).(T)
		if !ok {
			return nil, fmt.Errorf("parseJSON: cannot coerce string to %T", zero)
		}
		return &v, nil
	case gjson.Result:
		v, ok := any(gjson.ParseBytes(data)).(T)
		if !ok {
			return nil, fmt.Errorf("parseJSON: cannot coerce gjson.Result to %T", zero)
		}
		return &v, nil
	default:
		var result T
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, err
		}
		return &result, nil
	}
}
