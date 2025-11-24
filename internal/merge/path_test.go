package merge

import (
	"testing"
)

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

func TestValidateFieldPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		// Valid paths
		{
			name:    "root path",
			path:    "/",
			wantErr: false,
		},
		{
			name:    "single segment",
			path:    "/user",
			wantErr: false,
		},
		{
			name:    "multiple segments",
			path:    "/user/profile",
			wantErr: false,
		},
		{
			name:    "segment with dot",
			path:    "/user.info",
			wantErr: false,
		},
		{
			name:    "segment with multiple dots",
			path:    "/user.profile.name",
			wantErr: false,
		},
		{
			name:    "multiple segments with dots",
			path:    "/user.info/profile.data",
			wantErr: false,
		},
		{
			name:    "underscore prefix",
			path:    "/_private",
			wantErr: false,
		},
		{
			name:    "dollar prefix",
			path:    "/$config",
			wantErr: false,
		},
		{
			name:    "with numbers",
			path:    "/user123",
			wantErr: false,
		},
		{
			name:    "with underscore",
			path:    "/user_name",
			wantErr: false,
		},
		{
			name:    "with dollar sign",
			path:    "/user$val",
			wantErr: false,
		},
		{
			name:    "deep nesting",
			path:    "/a/b/c/d/e",
			wantErr: false,
		},
		{
			name:    "complex valid path",
			path:    "/_config/$value/data.info/item123",
			wantErr: false,
		},

		// Invalid paths
		{
			name:    "empty string",
			path:    "",
			wantErr: true,
		},
		{
			name:    "no leading slash",
			path:    "user",
			wantErr: true,
		},
		{
			name:    "trailing slash",
			path:    "/user/",
			wantErr: true,
		},
		{
			name:    "starts with number",
			path:    "/123",
			wantErr: true,
		},
		{
			name:    "contains hyphen",
			path:    "/user-name",
			wantErr: true,
		},
		{
			name:    "contains space",
			path:    "/user name",
			wantErr: true,
		},
		{
			name:    "segment starts with number",
			path:    "/user/123",
			wantErr: true,
		},
		{
			name:    "double slash",
			path:    "//",
			wantErr: true,
		},
		{
			name:    "double slash in middle",
			path:    "/user//profile",
			wantErr: true,
		},
		{
			name:    "contains @ symbol",
			path:    "/user@host",
			wantErr: true,
		},
		{
			name:    "contains # symbol",
			path:    "/user#tag",
			wantErr: true,
		},
		{
			name:    "starts with dot",
			path:    "/.config",
			wantErr: true,
		},
		{
			name:    "segment starts with dot",
			path:    "/user/.config",
			wantErr: true,
		},
		{
			name:    "only slash and slash",
			path:    "/user/",
			wantErr: true,
		},
		{
			name:    "chinese characters",
			path:    "/用户",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateFieldPath(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFieldPath(%q) error = %v, wantErr %v", tt.path, err, tt.wantErr)
			}
		})
	}
}
