package merge

import "testing"

func TestToGjsonPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "root path",
			path:     "/",
			expected: "",
		},
		{
			name:     "single segment",
			path:     "/user",
			expected: "user",
		},
		{
			name:     "multiple segments",
			path:     "/user/profile",
			expected: "user.profile",
		},
		{
			name:     "deep nesting",
			path:     "/a/b/c/d/e",
			expected: "a.b.c.d.e",
		},
		{
			name:     "underscore and dollar",
			path:     "/_private/$config",
			expected: "_private.$config",
		},
		{
			name:     "with numbers",
			path:     "/user123/item456",
			expected: "user123.item456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToGjsonPath(tt.path)
			if result != tt.expected {
				t.Errorf("toGjsonPath(%q) = %q, want %q", tt.path, result, tt.expected)
			}
		})
	}
}
