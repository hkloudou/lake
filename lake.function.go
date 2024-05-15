package lake

import (
	"fmt"
	"strings"
)

// Type is Result type
type Type int

const (
	// Null is a null json value
	Null Type = iota
	// False is a json false boolean
	False
	// Number is json number
	Number
	// String is a json string
	String
	// True is a json true boolean
	True
	// JSON is a raw block of JSON
	JSON
)

type Result struct {
	Type int
}

// GetMapValue 根据给定的路径（如"x.y.z"）递归地从map[string]interface{}中读取值。
// 如果值存在且可以转换为目标类型，则返回该值和true；否则，返回默认值和false。
func GetMapValue(data map[string]interface{}, path string) (interface{}, bool) {
	segments := strings.Split(path, ".")
	for i, segment := range segments {
		if val, exists := data[segment]; exists {
			if i == len(segments)-1 {
				return val, true
			}
			if nextMap, ok := val.(map[string]interface{}); ok {
				data = nextMap
			} else {
				return nil, false
			}
		} else {
			return nil, false
		}
	}
	return nil, false
}
func GetMapString(data map[string]interface{}, path string, defaultValue string) string {
	if val, ok := GetMapValue(data, path); ok {
		if stringVal, ok := val.(string); ok {
			return stringVal
		}
	}
	return defaultValue
}

func GetMapFloat64(data map[string]interface{}, path string, defaultValue float64) float64 {
	if val, ok := GetMapValue(data, path); ok {
		if stringVal, ok := val.(float64); ok {
			return stringVal
		}
	}
	return defaultValue
}

func GetMapInt(data map[string]interface{}, path string, defaultValue int) int {
	if val, ok := GetMapValue(data, path); ok {
		if stringVal, ok := val.(int); ok {
			return int(stringVal)
		}
	}
	return defaultValue
}

func updateResult(result *map[string]any, file *fileInfo) {

	current := *result

	for i, field := range file.Field {
		if i == len(file.Field)-1 { // Last
			// if current == nil {
			// 	// tmp := make(map[string]any)
			// 	// *result = tmp
			// 	current = make(map[string]any)
			// }
			if file.Merge == MergeTypeOver {
				if file.Value == nil {
					delete(current, field)
				} else {
					current[field] = file.Value
				}
			} else if file.Merge == MergeTypeUpsert {
				if _, ok := current[field]; !ok {
					current[field] = make(map[string]any)
				}
				for k, v := range file.Value.(map[string]any) {
					if v == nil {
						delete(current[field].(map[string]any), k)
					} else {
						current[field].(map[string]any)[k] = v
					}
				}
			}
		} else {
			if _, ok := current[field]; !ok {
				current[field] = make(map[string]any)
			}
			current = current[field].(map[string]any)
		}
	}
	if len(file.Field) == 0 { // Root directory operation
		if file.Merge == MergeTypeOver {
			if file.Value == nil {
				*result = make(map[string]any)
			} else {
				*result = file.Value.(map[string]any)
			}
		} else if file.Merge == MergeTypeUpsert {
			// if _, ok := (*result).(map[string]any); !ok {
			for k, v := range file.Value.(map[string]any) {
				if v == nil {
					delete((*result), k)
				} else {
					(*result)[k] = v
				}
			}
			// }
		} else {
			panic("unknow merge")
		}
	}
}

func MergaMap(result *map[string]any, fileds []string, value any, mergeType MergeType) error {
	current := *result
	if mergeType == MergeTypeUpsert {
		_, found := value.(map[string]any)
		if !found {
			return fmt.Errorf("value is not a map, but mergeType is Upsert")
		}
	}
	for i, field := range fileds {
		if i == len(fileds)-1 { // Last
			if mergeType == MergeTypeOver {
				if value == nil {
					delete(current, field)
				} else {
					current[field] = value
				}
			} else if mergeType == MergeTypeUpsert {
				if _, ok := current[field]; !ok {
					current[field] = make(map[string]any)
				}
				for k, v := range value.(map[string]any) {
					if v == nil {
						delete(current[field].(map[string]any), k)
					} else {
						current[field].(map[string]any)[k] = v
					}
				}
			}
		} else {
			if _, ok := current[field]; !ok {
				current[field] = make(map[string]any)
			}
			current = current[field].(map[string]any)
		}
	}
	if len(fileds) == 0 { // Root directory operation
		if mergeType == MergeTypeOver {
			if value == nil {
				*result = make(map[string]any)
			} else {
				*result = value.(map[string]any)
			}
		} else if mergeType == MergeTypeUpsert {
			for k, v := range value.(map[string]any) {
				if v == nil {
					delete((*result), k)
				} else {
					(*result)[k] = v
				}
			}
		} else {
			return fmt.Errorf("unknow mergeType: %v", mergeType)
		}
	}
	return nil
}
