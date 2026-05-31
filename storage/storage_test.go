package storage

import "testing"

// Kind.String must be exhaustive and unique per value: the client memoises
// storage per kind and logs/errors print it, so a Kind added later must never
// alias an existing label. This guards the invariant against a future refactor
// that collapses String() back to a non-exhaustive form.
func TestKindStringExhaustiveAndUnique(t *testing.T) {
	seen := map[string]Kind{}
	for k := Kind(0); k < 8; k++ {
		s := k.String()
		if s == "" {
			t.Errorf("Kind(%d).String() is empty", k)
		}
		if prev, dup := seen[s]; dup {
			t.Errorf("Kind(%d) and Kind(%d) both stringify to %q", prev, k, s)
		}
		seen[s] = k
	}
	if Delta.String() != "delta" || Snap.String() != "snap" {
		t.Errorf("labels: Delta=%q Snap=%q, want delta/snap", Delta, Snap)
	}
}
