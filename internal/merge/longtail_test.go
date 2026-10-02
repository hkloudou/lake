package merge

import (
	"bytes"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/hkloudou/lake/v3/internal/index"
)

func delta(mt index.MergeType, path, body string) index.DeltaInfo {
	return index.DeltaInfo{MergeType: mt, Path: path, Body: []byte(body)}
}

// TestRFC7396OntoNullTarget pins the fix for the null-wedge: two individually
// valid writes — a Replace of `null`, then an RFC7396 patch at the same scope
// — must merge per RFC 7396 (null target treated as {}), not fail every read
// of the catalog with jsonpatch's ErrBadJSONDoc.
func TestRFC7396OntoNullTarget(t *testing.T) {
	// Field scope: /user is null, then patched.
	out, err := Merge([]byte(`{}`), []index.DeltaInfo{
		delta(index.MergeTypeReplace, "/user", `null`),
		delta(index.MergeTypeRFC7396, "/user", `{"a":1}`),
	})
	if err != nil {
		t.Fatalf("field-scope merge onto null: %v", err)
	}
	if string(out) != `{"user":{"a":1}}` {
		t.Fatalf("field-scope merge onto null = %s", out)
	}

	// Root scope: whole document replaced with null, then patched.
	out, err = Merge([]byte(`{}`), []index.DeltaInfo{
		delta(index.MergeTypeReplace, "/", `null`),
		delta(index.MergeTypeRFC7396, "/", `{"a":1}`),
	})
	if err != nil {
		t.Fatalf("root merge onto null: %v", err)
	}
	if string(out) != `{"a":1}` {
		t.Fatalf("root merge onto null = %s", out)
	}

	// An RFC7396 patch body of literal null nulls the scope (RFC: non-object
	// patch replaces) — and the scope must still be patchable afterwards.
	out, err = Merge([]byte(`{"user":{"x":2}}`), []index.DeltaInfo{
		delta(index.MergeTypeRFC7396, "/user", `null`),
		delta(index.MergeTypeRFC7396, "/user", `{"a":1}`),
	})
	if err != nil {
		t.Fatalf("null patch then object patch: %v", err)
	}
	if string(out) != `{"user":{"a":1}}` {
		t.Fatalf("null patch then object patch = %s", out)
	}

	// Scalar and array targets keep their RFC behavior (replaced by the
	// patch): the null normalization must not widen.
	out, err = Merge([]byte(`{"user":5}`), []index.DeltaInfo{
		delta(index.MergeTypeRFC7396, "/user", `{"a":1}`),
	})
	if err != nil {
		t.Fatalf("scalar target: %v", err)
	}
	if string(out) != `{"user":{"a":1}}` {
		t.Fatalf("scalar target = %s", out)
	}
}

// TestRFC7396PrecombineUnsound pins WHY the engine applies patches one by one:
// jsonpatch.MergeMergePatches is not equivalent to sequential application —
// a delete-then-set-object pair, precombined, resurrects deleted keys. Any
// future "optimization" that batches consecutive RFC7396 patches must fail
// this test's premise.
func TestRFC7396PrecombineUnsound(t *testing.T) {
	doc := []byte(`{"k":{"old":2}}`)
	p1, p2 := []byte(`{"k":null}`), []byte(`{"k":{"a":1}}`)

	s1, err := jsonpatch.MergePatch(doc, p1)
	if err != nil {
		t.Fatal(err)
	}
	sequential, err := jsonpatch.MergePatch(s1, p2)
	if err != nil {
		t.Fatal(err)
	}
	combined, err := jsonpatch.MergeMergePatches(p1, p2)
	if err != nil {
		t.Fatal(err)
	}
	precombined, err := jsonpatch.MergePatch(doc, combined)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(sequential, precombined) {
		t.Fatalf("MergeMergePatches became sequential-equivalent (%s) — precombining may now be safe, revisit engine.go", sequential)
	}
	if string(sequential) != `{"k":{"a":1}}` {
		t.Fatalf("sequential application = %s, want {\"k\":{\"a\":1}}", sequential)
	}
}

// TestMergeRootReplaceDoesNotAliasBody: the merged document a caller may
// mutate must never share backing with the delta Body retained on the
// ListResult.
func TestMergeRootReplaceDoesNotAliasBody(t *testing.T) {
	body := []byte(`{"a":1}`)
	entries := []index.DeltaInfo{{MergeType: index.MergeTypeReplace, Path: "/", Body: body}}
	out, err := Merge([]byte(`{}`), entries)
	if err != nil {
		t.Fatal(err)
	}
	if &out[0] == &body[0] {
		t.Fatal("root Replace returned the delta Body slice itself")
	}
	out[1] = 'X'
	if string(body) != `{"a":1}` {
		t.Fatalf("mutating the merged doc corrupted the delta Body: %s", body)
	}
}

// TestMergeRFC7396LiteralDoesNotAliasBody: evanphx MergePatch returns the
// patch slice ITSELF when the patch body is a non-object literal (null /
// number / string / bool). The engine must never hand that alias to the
// caller (who may mutate it) nor treat it as an owned buffer for in-place
// splices — either would corrupt the Body retained on the ListResult.
func TestMergeRFC7396LiteralDoesNotAliasBody(t *testing.T) {
	for _, lit := range []string{`null`, `5`, `"hello"`, `true`} {
		body := []byte(lit)
		out, err := Merge([]byte(`{"a":1}`), []index.DeltaInfo{
			{MergeType: index.MergeTypeRFC7396, Path: "/", Body: body},
		})
		if err != nil {
			t.Fatalf("literal %s: %v", lit, err)
		}
		if len(out) > 0 && &out[0] == &body[0] {
			t.Fatalf("literal %s: merge returned the delta Body slice itself", lit)
		}
		out[0] = 'X'
		if string(body) != lit {
			t.Fatalf("literal %s: mutating the merged doc corrupted the Body: %s", lit, body)
		}
	}
}
