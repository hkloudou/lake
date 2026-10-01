package lake

import (
	"context"
	"strings"
	"testing"
)

func TestReadHelpers_RejectNilList(t *testing.T) {
	ctx := context.Background()
	for name, err := range map[string]error{
		"ReadBytes":  second(ReadBytes(ctx, nil)),
		"ReadString": second(ReadString(ctx, nil)),
		"ReadMap":    second(ReadMap(ctx, nil)),
		"Read[T]":    second(Read[map[string]any](ctx, nil)),
	} {
		if err == nil || !strings.Contains(err.Error(), "requires a ListResult") {
			t.Fatalf("%s(nil) err = %v, want list requirement error", name, err)
		}
	}
}

func second[T any](_ T, err error) error { return err }
