package utils

import "testing"

func TestValidateCatalog_Accepted(t *testing.T) {
	cases := []string{
		"users",
		"USERS",
		"Users",
		"u",
		"u1",
		"a-b",
		"a_b",
		"a.b",
		"a1.b2-c3_d",
		"tenantA/users",
		"tenant/sub/leaf",
		"_users",     // underscore-led segment OK
		"123-tenant", // digit-led segment OK
		"v3.0.0-alpha.1",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			if err := ValidateCatalog(c); err != nil {
				t.Fatalf("expected accept, got error: %v", err)
			}
		})
	}
}

func TestValidateCatalog_Rejected(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"redis_delim", "tenant:users"},
		{"member_delim", "ten|users"},
		{"oss_marker_open", "(users"},
		{"oss_marker_close", ")users"},
		{"leading_slash", "/users"},
		{"trailing_slash", "users/"},
		{"double_slash", "tenant//users"},
		{"only_slash", "/"},
		{"leading_dash", "-tenant"},
		{"leading_dot", ".tenant"},
		{"leading_dash_in_segment", "tenant/-users"},
		{"leading_dot_in_segment", "tenant/.users"},
		{"plus", "tenant+users"},
		{"equals", "tenant=users"},
		{"at", "tenant@users"},
		{"hash", "tenant#users"},
		{"ampersand", "tenant&users"},
		{"space", "tenant users"},
		{"tab", "tenant\tusers"},
		{"unicode", "用户"},
		{"backslash", "tenant\\users"},
		{"asterisk", "tenant*users"},
		{"question_mark", "tenant?users"},
		{"control_char", "tenant\x00users"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := ValidateCatalog(c.in); err == nil {
				t.Fatalf("expected reject for %q, got nil error", c.in)
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
		{"root path", "/", false},
		{"single segment", "/user", false},
		{"multiple segments", "/user/profile", false},
		{"segment with dot", "/user.info", false},
		{"segment with multiple dots", "/user.profile.name", false},
		{"multiple segments with dots", "/user.info/profile.data", false},
		{"underscore prefix", "/_private", false},
		{"dollar prefix", "/$config", false},
		{"with numbers", "/user123", false},
		{"with underscore", "/user_name", false},
		{"with dollar sign", "/user$val", false},
		{"deep nesting", "/a/b/c/d/e", false},
		{"complex valid path", "/_config/$value/data.info/item123", false},

		// Invalid paths
		{"empty string", "", true},
		{"no leading slash", "user", true},
		{"trailing slash", "/user/", true},
		{"starts with number", "/123", true},
		{"contains hyphen", "/user-name", true},
		{"contains space", "/user name", true},
		{"segment starts with number", "/user/123", true},
		{"double slash", "//", true},
		{"double slash in middle", "/user//profile", true},
		{"contains @ symbol", "/user@host", true},
		{"contains # symbol", "/user#tag", true},
		{"starts with dot", "/.config", true},
		{"segment starts with dot", "/user/.config", true},
		{"contains chinese characters", "/用户", true},
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
