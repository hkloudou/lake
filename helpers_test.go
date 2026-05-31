package lake

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// parseJSON has four code paths (raw []byte, raw string, gjson.Result,
// JSON unmarshal). Cover each, plus a regression test for the previous
// unsafe.Pointer implementation's "(nil, nil) on gjson type assert
// failure" silent bug — the new implementation must surface a real error
// in any path where the conversion fails.

func TestParseJSON_RawBytes(t *testing.T) {
	src := []byte(`{"x":1}`)
	out, err := parseJSON[[]byte](src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil result")
	}
	if !reflect.DeepEqual(*out, src) {
		t.Fatalf("want %q, got %q", src, *out)
	}
}

func TestParseJSON_RawString(t *testing.T) {
	src := []byte(`{"x":1}`)
	out, err := parseJSON[string](src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil result")
	}
	if *out != string(src) {
		t.Fatalf("want %q, got %q", string(src), *out)
	}
}

func TestParseJSON_GjsonResult(t *testing.T) {
	src := []byte(`{"name":"Alice","age":30}`)
	out, err := parseJSON[gjson.Result](src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil result")
	}
	if got := out.Get("name").String(); got != "Alice" {
		t.Fatalf("name: want Alice, got %q", got)
	}
	if got := out.Get("age").Int(); got != 30 {
		t.Fatalf("age: want 30, got %d", got)
	}
}

func TestParseJSON_TypedStruct(t *testing.T) {
	type User struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	src := []byte(`{"name":"Bob","age":42}`)
	out, err := parseJSON[User](src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil result")
	}
	if out.Name != "Bob" || out.Age != 42 {
		t.Fatalf("want {Bob 42}, got %+v", *out)
	}
}

func TestParseJSON_TypedMap(t *testing.T) {
	src := []byte(`{"a":1,"b":2}`)
	out, err := parseJSON[map[string]int](src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil result")
	}
	if (*out)["a"] != 1 || (*out)["b"] != 2 {
		t.Fatalf("want {a:1 b:2}, got %v", *out)
	}
}

func TestParseJSON_InvalidJSONReturnsError(t *testing.T) {
	type User struct{ Name string }
	out, err := parseJSON[User]([]byte(`{not json`))
	if err == nil {
		t.Fatal("expected error on invalid JSON, got nil")
	}
	// Regression check: must not silently return (nil, nil) like the old
	// unsafe.Pointer-based gjson branch did on type-assert failure.
	if out != nil && err == nil {
		t.Fatal("must not return (non-nil, nil) on parse failure")
	}
	if out == nil && err == nil {
		t.Fatal("must not return (nil, nil) — silent failure regression")
	}
}

func TestParseJSON_NoSilentNilNil(t *testing.T) {
	// The previous unsafe.Pointer impl had `return nil, nil` in the gjson
	// type-assert fallback. Because gjson.Result is the actual T here,
	// the assertion always succeeds; this test is structured so a future
	// refactor that re-introduces a silent (nil, nil) path would fail.
	out, err := parseJSON[gjson.Result]([]byte(`{}`))
	if err == nil && out == nil {
		t.Fatal("(nil, nil) is not an acceptable parseJSON return")
	}
}

func TestReadHelpers_RejectNilList(t *testing.T) {
	ctx := context.Background()

	if _, err := ReadBytes(ctx, nil); err == nil || !strings.Contains(err.Error(), "requires a ListResult") {
		t.Fatalf("ReadBytes(nil) err = %v, want list requirement error", err)
	}
	if _, err := ReadString(ctx, nil); err == nil || !strings.Contains(err.Error(), "requires a ListResult") {
		t.Fatalf("ReadString(nil) err = %v, want list requirement error", err)
	}
	if _, err := ReadMap(ctx, nil); err == nil || !strings.Contains(err.Error(), "requires a ListResult") {
		t.Fatalf("ReadMap(nil) err = %v, want list requirement error", err)
	}
	if _, err := Read[gjson.Result](ctx, nil); err == nil || !strings.Contains(err.Error(), "requires a ListResult") {
		t.Fatalf("Read[T](nil) err = %v, want list requirement error", err)
	}
}
