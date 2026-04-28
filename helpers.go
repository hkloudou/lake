package lake

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tidwall/gjson"
)

// ReadBytes returns the merged document as raw bytes.
func ReadBytes(ctx context.Context, list *ListResult) ([]byte, error) {
	return list.client.readData(ctx, list)
}

// ReadString returns the merged document as a JSON string.
func ReadString(ctx context.Context, list *ListResult) (string, error) {
	data, err := list.client.readData(ctx, list)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ReadMap returns the merged document parsed as a generic map.
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

// Read returns the merged document parsed into a value of type T.
//
// For T == []byte / string / gjson.Result the raw merged bytes are
// returned wrapped in T (no JSON unmarshal). For all other T, encoding/json
// is used.
func Read[T any](ctx context.Context, list *ListResult) (*T, error) {
	data, err := list.client.readData(ctx, list)
	if err != nil {
		return nil, err
	}
	return parseJSON[T](data)
}

// parseJSON converts merged bytes into a value of type T.
//
// Special cases (no JSON unmarshal):
//   - T = []byte         → returned as-is, wrapped in T
//   - T = string         → string(data) wrapped in T
//   - T = gjson.Result   → gjson.ParseBytes(data) wrapped in T
//
// Default: json.Unmarshal into a fresh T.
//
// Implemented via a type switch on a zero T; this replaces an earlier
// unsafe.Pointer-based implementation that was both fragile under future
// Go layout changes and silently returned (nil, nil) when a type
// assertion failed in the gjson branch.
func parseJSON[T any](data []byte) (*T, error) {
	var zero T
	switch any(zero).(type) {
	case []byte:
		v, ok := any(data).(T)
		if !ok {
			return nil, fmt.Errorf("parseJSON: failed to coerce []byte to %T", zero)
		}
		return &v, nil

	case string:
		v, ok := any(string(data)).(T)
		if !ok {
			return nil, fmt.Errorf("parseJSON: failed to coerce string to %T", zero)
		}
		return &v, nil

	case gjson.Result:
		v, ok := any(gjson.ParseBytes(data)).(T)
		if !ok {
			return nil, fmt.Errorf("parseJSON: failed to coerce gjson.Result to %T", zero)
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
