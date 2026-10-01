package lake

import (
	"context"
	"encoding/json"
	"errors"
)

var errNoList = errors.New("lake: Read requires a ListResult from List/BatchList")

// ReadBytes returns the merged document as raw JSON bytes.
func ReadBytes(ctx context.Context, list *ListResult) ([]byte, error) {
	if list == nil || list.client == nil {
		return nil, errNoList
	}
	return list.client.readData(ctx, list)
}

// ReadString returns the merged document as a JSON string.
func ReadString(ctx context.Context, list *ListResult) (string, error) {
	data, err := ReadBytes(ctx, list)
	return string(data), err
}

// ReadMap returns the merged document parsed as map[string]any.
func ReadMap(ctx context.Context, list *ListResult) (map[string]any, error) {
	out, err := Read[map[string]any](ctx, list)
	if err != nil {
		return nil, err
	}
	return *out, nil
}

// Read returns the merged document unmarshalled into a T.
func Read[T any](ctx context.Context, list *ListResult) (*T, error) {
	data, err := ReadBytes(ctx, list)
	if err != nil {
		return nil, err
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return &v, nil
}
