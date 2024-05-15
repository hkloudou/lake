package lake

import (
	"testing"
)

func Test_GetMapValue(t *testing.T) {
	var data = map[string]any{
		"string": "value",
		"int":    1,
		"float":  0.1,
		"map": map[string]any{
			"key": "value2",
		},
	}
	if val, ok := GetMapValue(data, "string"); !ok || val != "value" {
		t.Fatal("GetMapValue failed")
	}
	if val, ok := GetMapValue(data, "int"); !ok || val != 1 {
		t.Fatal("GetMapValue failed")
	}
	if val, ok := GetMapValue(data, "float"); !ok || val != 0.1 {
		t.Fatal("GetMapValue failed")
	}
	if val, ok := GetMapValue(data, "map.key"); !ok || val != "value2" {
		t.Fatal("GetMapValue failed")
	}
}

func Test_GetMapInt(t *testing.T) {
	var data = map[string]any{
		"string": "value",
		"int":    1,
		"float":  2.1,
		"map": map[string]any{
			"key": "value2",
		},
	}
	if GetMapInt(data, "string", 22) != 22 {
		t.Fatal("GetMapInt failed")
	}
	if GetMapInt(data, "int", 22) != 1 {
		t.Fatal("GetMapInt failed")
	}
	if GetMapInt(data, "float", 22) != 1 {
		t.Fatal("GetMapInt failed")
	}
}
