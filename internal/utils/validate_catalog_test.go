package utils

import (
	"strings"
	"testing"
)

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
		strings.Repeat("a", MaxCatalogLen), // exactly at the length cap
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
		{"segment with dot", "/user.info", true},
		{"segment with multiple dots", "/user.profile.name", true},
		{"multiple segments with dots", "/user.info/profile.data", true},
		{"underscore prefix", "/_private", false},
		{"dollar prefix", "/$config", false},
		{"with numbers", "/user123", false},
		{"with underscore", "/user_name", false},
		{"with dollar sign", "/user$val", false},
		{"deep nesting", "/a/b/c/d/e", false},
		{"complex valid path", "/_config/$value/data_info/item123", false},

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

// TestLengthCaps: names are capped at creation and on decode alike (one
// validator, one rule), and the charset rules apply on top of the cap.
func TestLengthCaps(t *testing.T) {
	if err := ValidateCatalog(strings.Repeat("a", MaxCatalogLen)); err != nil {
		t.Errorf("ValidateCatalog at cap: unexpected error: %v", err)
	}
	if err := ValidateCatalog(strings.Repeat("a", MaxCatalogLen+1)); err == nil {
		t.Error("ValidateCatalog over cap: expected error, got nil")
	}
	if err := ValidateFieldPath("/" + strings.Repeat("a", MaxFieldPathLen-1)); err != nil {
		t.Errorf("ValidateFieldPath at cap: unexpected error: %v", err)
	}
	if err := ValidateFieldPath("/" + strings.Repeat("a", MaxFieldPathLen)); err == nil {
		t.Error("ValidateFieldPath over cap: expected error, got nil")
	}
	if err := ValidateCatalog("ten:ant"); err == nil {
		t.Error("ValidateCatalog bad charset: expected error, got nil")
	}
	if err := ValidateFieldPath("no-slash"); err == nil {
		t.Error("ValidateFieldPath bad shape: expected error, got nil")
	}
}
