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
			name:     "segment with dot",
			path:     "/user.info",
			expected: `user\.info`,
		},
		{
			name:     "segment with multiple dots",
			path:     "/user.profile.name",
			expected: `user\.profile\.name`,
		},
		{
			name:     "multiple segments with dots",
			path:     "/user.info/profile.data",
			expected: `user\.info.profile\.data`,
		},
		{
			name:     "deep nesting",
			path:     "/a/b/c/d/e",
			expected: "a.b.c.d.e",
		},
		{
			name:     "complex path with dots and slashes",
			path:     "/config.app/database.settings/host.name",
			expected: `config\.app.database\.settings.host\.name`,
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
