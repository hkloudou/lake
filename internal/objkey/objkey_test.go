package objkey

import (
	"strings"
	"testing"
)

// The object path and URI shapes are part of Lake's DATA SPEC: every recorded
// delta/snap URI must stay fetchable by future versions (and by plain
// S3/OSS tooling), so these tests pin the exact rendered forms — a change
// that breaks one of them breaks already-written deployments.

func TestDeltaAndSnapPathShape(t *testing.T) {
	// md5("users") = 9bc65c2abec141778ffaa729489f3e87 → shard "9bc6";
	// pure-lowercase catalog → "(users".
	const uuid = "0123456789abcdef0123456789abcdef"
	if got, want := DeltaPath("users", uuid), "9bc6/(users/"+uuid+".dat"; got != want {
		t.Fatalf("DeltaPath = %q, want %q", got, want)
	}
	if got, want := SnapPath("users", "1700000000_42"), "9bc6/(users/1700000000_42.snap"; got != want {
		t.Fatalf("SnapPath = %q, want %q", got, want)
	}
}

func TestCatalogEncodingForms(t *testing.T) {
	cases := []struct {
		catalog string
		enc     string // the encoded segment (2nd path element)
	}{
		{"users", "(users"},         // pure lowercase → "(" prefix
		{"USERS", ")USERS"},         // pure uppercase → ")" prefix
		{"a-b_c/d.e", "(a-b_c/d.e"}, // lowercase incl. - _ / . stays readable
		{"Users", "kvzwk4tt"},       // mixed case → lowercased base32, no padding
		{"u2", "(u2"},               // digits are case-safe
	}
	for _, tc := range cases {
		path := DeltaPath(tc.catalog, "0123456789abcdef0123456789abcdef")
		parts := strings.SplitN(path, "/", 2)
		rest := parts[1]
		if !strings.HasPrefix(rest, tc.enc+"/") {
			t.Errorf("catalog %q: encoded path %q, want segment %q", tc.catalog, rest, tc.enc)
		}
	}

	// The three forms must never collide: "(x" / ")X" markers are forbidden
	// inside catalog names by ValidateCatalog, and base32 output is bare
	// lowercase alphanumerics (no marker).
	if DeltaPath("users", "u") == DeltaPath("USERS", "u") {
		t.Fatal("lower/upper encodings must not collide")
	}
}

func TestBuildParseURIRoundTrip(t *testing.T) {
	uri := BuildURI("oss", "my-bucket", "9bc6/(users/x.dat")
	if uri != "oss://my-bucket/9bc6/(users/x.dat" {
		t.Fatalf("BuildURI = %q", uri)
	}
	p, b, path, err := ParseURI(uri)
	if err != nil || p != "oss" || b != "my-bucket" || path != "9bc6/(users/x.dat" {
		t.Fatalf("ParseURI = (%q,%q,%q,%v)", p, b, path, err)
	}
}

func TestParseURIRejectsMalformed(t *testing.T) {
	for _, uri := range []string{
		"",                 // empty
		"oss://",           // no bucket/path
		"oss://bucket",     // no path
		"://bucket/path",   // empty provider
		"oss:/bucket/path", // not a "://"
		"bucket/path",      // no scheme at all
	} {
		if _, _, _, err := ParseURI(uri); err == nil {
			t.Errorf("ParseURI(%q): expected error, got nil", uri)
		}
	}
}
