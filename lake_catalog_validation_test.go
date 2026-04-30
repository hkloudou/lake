package lake

import (
	"context"
	"strings"
	"testing"
)

// V3: each public API that takes a catalog must reject invalid names
// *before* touching Redis or OSS. We point at unreachable Redis: a
// valid catalog surfaces a connection error, an invalid catalog must
// surface a validation error first.

const unreachableRedis = "127.0.0.1:1"

func newDeadClient(t *testing.T) *Client {
	t.Helper()
	return NewLake(unreachableRedis)
}

func isValidationErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "invalid catalog")
}

func TestCatalogValidation_WriteBegin(t *testing.T) {
	c := newDeadClient(t)

	_, err := c.WriteBegin(context.Background(), WriteBeginRequest{
		Catalog:   "/leading-slash",
		Path:      "/x",
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

	// Internal "/" is allowed. Call should fail later (Redis unreachable
	// → ensureInitialized fails) but NOT at validation.
	_, err := c.WriteBegin(context.Background(), WriteBeginRequest{
		Catalog:   "tenantA/users",
		Path:      "/x",
		MergeType: MergeTypeReplace,
	})
	if isValidationErr(err) {
		t.Fatalf("internal / should be allowed; got validation error: %v", err)
	}
	if err == nil {
		t.Fatal("expected non-validation error (Redis unreachable), got nil")
	}
}
