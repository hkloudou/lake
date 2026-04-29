package lake

import (
	"context"
	"strings"
	"testing"
)

// Each public API that takes a catalog must reject invalid names *before*
// touching Redis. We verify by pointing at an unreachable Redis: a valid
// catalog would surface a connection error, an invalid catalog must
// surface a validation error first.

const unreachableRedis = "127.0.0.1:1"

func newDeadClient(t *testing.T) *Client {
	t.Helper()
	c := NewLake(unreachableRedis)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func isValidationErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "invalid catalog")
}

func TestCatalogValidation_Write(t *testing.T) {
	c := newDeadClient(t)

	err := c.Write(context.Background(), WriteRequest{
		Catalog:   "/leading-slash",
		Path:      "/x",
		Body:      []byte(`1`),
		MergeType: MergeTypeReplace,
	})
	if !isValidationErr(err) {
		t.Fatalf("expected catalog validation error, got %v", err)
	}
}

func TestCatalogValidation_List(t *testing.T) {
	c := newDeadClient(t)

	res := c.List(context.Background(), "bad:name")
	if !isValidationErr(res.Err) {
		t.Fatalf("expected catalog validation error in ListResult.Err, got %v", res.Err)
	}
}

func TestCatalogValidation_BatchListMixesGoodAndBad(t *testing.T) {
	c := newDeadClient(t)

	// Bad name gets a per-result validation error, good name gets a
	// downstream init/connection error (since Redis is unreachable).
	out := c.BatchList(context.Background(), []string{"good", "bad|name"})
	if got := len(out); got != 2 {
		t.Fatalf("expected 2 results, got %d", got)
	}
	if !isValidationErr(out["bad|name"].Err) {
		t.Fatalf("expected validation error for bad name, got %v", out["bad|name"].Err)
	}
	if out["good"].Err == nil {
		t.Fatalf("expected init/connection error for good name (Redis unreachable), got nil")
	}
	if isValidationErr(out["good"].Err) {
		t.Fatalf("good name should not get a validation error, got %v", out["good"].Err)
	}
}

func TestCatalogValidation_ClearHistory(t *testing.T) {
	c := newDeadClient(t)

	err := c.ClearHistory(context.Background(), "tenant//users")
	if !isValidationErr(err) {
		t.Fatalf("expected catalog validation error, got %v", err)
	}
}

func TestCatalogValidation_AcceptsHierarchy(t *testing.T) {
	c := newDeadClient(t)

	// Internal "/" is allowed; the call should fail later (unreachable Redis)
	// but NOT at the validation step.
	err := c.Write(context.Background(), WriteRequest{
		Catalog:   "tenantA/users",
		Path:      "/x",
		Body:      []byte(`1`),
		MergeType: MergeTypeReplace,
	})
	if isValidationErr(err) {
		t.Fatalf("internal / should be allowed; got validation error: %v", err)
	}
	if err == nil {
		t.Fatal("expected non-validation error (Redis unreachable), got nil")
	}
}
