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
		"_users",        // underscore-led segment OK
		"123-tenant",    // digit-led segment OK
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
