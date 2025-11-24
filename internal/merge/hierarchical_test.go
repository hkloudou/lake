package merge

import (
	"testing"

	"github.com/hkloudou/lake/v2/internal/index"
)

// TestGetParentPaths tests parent path extraction
func TestGetParentPaths(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		expected []string
	}{
		{
			name:     "root path",
			field:    "/",
			expected: []string{},
		},
		{
			name:     "single level",
			field:    "/base",
			expected: []string{"/"},
		},
		{
			name:     "two levels",
			field:    "/base/child",
			expected: []string{"/", "/base"},
		},
		{
			name:     "three levels",
			field:    "/base/child/item",
			expected: []string{"/", "/base", "/base/child"},
		},
		{
			name:     "four levels",
			field:    "/a/b/c/d",
			expected: []string{"/", "/a", "/a/b", "/a/b/c"},
		},
		{
			name:     "with dots",
			field:    "/base.info/child.data",
			expected: []string{"/", "/base.info"},
		},
		{
			name:     "deep nesting",
			field:    "/a/b/c/d/e/f",
			expected: []string{"/", "/a", "/a/b", "/a/b/c", "/a/b/c/d", "/a/b/c/d/e"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getParentPaths(tt.field)
			if len(result) != len(tt.expected) {
				t.Errorf("Length mismatch: got %d, want %d", len(result), len(tt.expected))
				t.Logf("Got: %v", result)
				t.Logf("Want: %v", tt.expected)
				return
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("Parent[%d]: got %q, want %q", i, result[i], tt.expected[i])
				}
			}
		})
	}
}

// TestHierarchicalUpdateMap tests hierarchical update propagation
func TestHierarchicalUpdateMap(t *testing.T) {
	hm := NewHierarchicalUpdateMap()

	// Update /base/child1
	ts1 := index.TimeSeqID{Timestamp: 1700000000, SeqID: 1}
	hm.Update("/base/child1", ts1)

	// Check updates (field + /base + /)
	all := hm.GetAll()
	if len(all) != 3 {
		t.Errorf("Expected 3 updates, got %d", len(all))
	}
	if all["/base/child1"] != ts1 {
		t.Errorf("/base/child1: got %v, want %v", all["/base/child1"], ts1)
	}
	if all["/base"] != ts1 {
		t.Errorf("/base: got %v, want %v", all["/base"], ts1)
	}
	if all["/"] != ts1 {
		t.Errorf("/: got %v, want %v", all["/"], ts1)
	}

	// Update /base/child2 with newer timestamp
	ts2 := index.TimeSeqID{Timestamp: 1700000000, SeqID: 2}
	hm.Update("/base/child2", ts2)

	// Check that /base and / are updated to newer timestamp
	all = hm.GetAll()
	if len(all) != 4 {
		t.Errorf("Expected 4 updates, got %d", len(all))
	}
	if all["/base"] != ts2 {
		t.Errorf("/base should be updated to ts2, got %v, want %v", all["/base"], ts2)
	}
	if all["/"] != ts2 {
		t.Errorf("/ should be updated to ts2, got %v, want %v", all["/"], ts2)
	}

	// Update /base/child1 with older timestamp (should not update /base)
	ts0 := index.TimeSeqID{Timestamp: 1699999999, SeqID: 999}
	hm.Update("/base/child1", ts0)

	all = hm.GetAll()
	// /base/child1 should still be ts1 (newer)
	if all["/base/child1"] != ts1 {
		t.Errorf("/base/child1 should remain ts1, got %v", all["/base/child1"])
	}
	// /base should still be ts2 (newest)
	if all["/base"] != ts2 {
		t.Errorf("/base should remain ts2, got %v", all["/base"])
	}
	// / should still be ts2 (newest)
	if all["/"] != ts2 {
		t.Errorf("/ should remain ts2, got %v", all["/"])
	}

	t.Logf("✓ Hierarchical update map working correctly")
	t.Logf("Final state: %+v", all)
}

// TestHierarchicalUpdateMapComplex tests complex hierarchical updates
func TestHierarchicalUpdateMapComplex(t *testing.T) {
	hm := NewHierarchicalUpdateMap()

	// Simulate multiple updates at different levels
	updates := []struct {
		field string
		ts    index.TimeSeqID
	}{
		{"/a/b/c", index.TimeSeqID{Timestamp: 100, SeqID: 1}},
		{"/a/b/d", index.TimeSeqID{Timestamp: 100, SeqID: 2}},
		{"/a/e", index.TimeSeqID{Timestamp: 100, SeqID: 3}},
		{"/a/b/c/deep", index.TimeSeqID{Timestamp: 100, SeqID: 4}},
	}

	for _, u := range updates {
		hm.Update(u.field, u.ts)
	}

	all := hm.GetAll()

	// /a should be ts4 (newest)
	expected := index.TimeSeqID{Timestamp: 100, SeqID: 4}
	if all["/a"] != expected {
		t.Errorf("/a: got %v, want %v", all["/a"], expected)
	}

	// /a/b should be ts4 (newest in subtree)
	if all["/a/b"] != expected {
		t.Errorf("/a/b: got %v, want %v", all["/a/b"], expected)
	}

	// /a/b/c should be ts4 (from /a/b/c/deep)
	if all["/a/b/c"] != expected {
		t.Errorf("/a/b/c: got %v, want %v", all["/a/b/c"], expected)
	}

	// / (root) should be ts4 (newest of all)
	if all["/"] != expected {
		t.Errorf("/: got %v, want %v", all["/"], expected)
	}

	t.Logf("✓ Complex hierarchical updates working correctly")
	for k, v := range all {
		t.Logf("  %s → %s (score: %f)", k, v.String(), v.Score())
	}
}

// TestHierarchicalUpdateMapWithDots tests paths containing dots
func TestHierarchicalUpdateMapWithDots(t *testing.T) {
	hm := NewHierarchicalUpdateMap()

	ts1 := index.TimeSeqID{Timestamp: 1000, SeqID: 1}
	ts2 := index.TimeSeqID{Timestamp: 1000, SeqID: 2}

	// Update paths with dots in field names
	hm.Update("/user.info/profile.data", ts1)
	hm.Update("/user.info/settings.prefs", ts2)

	all := hm.GetAll()

	// /user.info should be ts2 (newer)
	if all["/user.info"] != ts2 {
		t.Errorf("/user.info: got %v, want %v", all["/user.info"], ts2)
	}

	// Both leaf nodes should exist
	if all["/user.info/profile.data"] != ts1 {
		t.Errorf("/user.info/profile.data: got %v, want %v", all["/user.info/profile.data"], ts1)
	}
	if all["/user.info/settings.prefs"] != ts2 {
		t.Errorf("/user.info/settings.prefs: got %v, want %v", all["/user.info/settings.prefs"], ts2)
	}

	// / (root) should be ts2 (newest)
	if all["/"] != ts2 {
		t.Errorf("/: got %v, want %v", all["/"], ts2)
	}

	t.Logf("✓ Hierarchical updates with dots working correctly")
}
