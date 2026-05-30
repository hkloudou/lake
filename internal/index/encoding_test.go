package index

import (
	"encoding/json"
	"testing"
)

const testURI = "oss://my-bucket/4f3a/(users/0123456789abcdef.dat"

// mkMember builds a delta member the way the notify Lua script does:
// a JSON array [mergeType, fieldPath, tsSeq, uri].
func mkMember(mt int, path, tsSeq, uri string) string {
	b, _ := json.Marshal([]any{mt, path, tsSeq, uri})
	return string(b)
}

func TestDecodeMember(t *testing.T) {
	u := testURI
	tests := []struct {
		member          string
		score           float64
		expectField     string
		expectMergeType MergeType
		expectTsSeq     TimeSeqID
		expectURI       string
		expectError     bool
	}{
		{mkMember(1, "/user/name", "1700000000_1", u), 1700000000.000001, "/user/name", MergeTypeReplace, TimeSeqID{1700000000, 1}, u, false},
		{mkMember(2, "/profile", "1700000000_2", u), 1700000000.000002, "/profile", MergeTypeRFC7396, TimeSeqID{1700000000, 2}, u, false},
		// Invalid formats
		{"not json", 0, "", 0, TimeSeqID{}, "", true},
		{`[1,"/x"]`, 0, "", 0, TimeSeqID{}, "", true},                                            // too few elements
		{`[1,"/x","1700000000_1","oss://b/k","extra"]`, 0, "", 0, TimeSeqID{}, "", true},         // too many
		{mkMember(0, "/x", "1700000000_1", u), 0, "", 0, TimeSeqID{}, "", true},                  // merge type 0
		{mkMember(3, "/x", "1700000000_1", u), 0, "", 0, TimeSeqID{}, "", true},                  // merge type 3 (RFC6902 removed)
		{mkMember(1, "x", "1700000000_1", u), 1700000000.000001, "", 0, TimeSeqID{}, "", true},   // bad path (no leading /)
		{mkMember(1, "/x", "invalid", u), 0, "", 0, TimeSeqID{}, "", true},                       // bad tsSeq
		{mkMember(1, "/x", "1700000000_1", ""), 1700000000.000001, "", 0, TimeSeqID{}, "", true}, // empty uri
		{mkMember(1, "/x", "1700000000_1", u), 1700000000.000002, "", 0, TimeSeqID{}, "", true},  // score mismatch
	}
	for _, tt := range tests {
		d, err := DecodeDeltaMember(tt.member, tt.score)
		if tt.expectError {
			if err == nil {
				t.Errorf("DecodeDeltaMember(%q, %.6f) expected error, got nil", tt.member, tt.score)
			}
			continue
		}
		if err != nil {
			t.Errorf("DecodeDeltaMember(%q, %.6f) unexpected error: %v", tt.member, tt.score, err)
			continue
		}
		if d.Path != tt.expectField || d.MergeType != tt.expectMergeType || d.TsSeq != tt.expectTsSeq || d.URI != tt.expectURI {
			t.Errorf("DecodeDeltaMember(%q) = (path=%q, type=%d, tsSeq=%v, uri=%q), want (path=%q, type=%d, tsSeq=%v, uri=%q)",
				tt.member, d.Path, d.MergeType, d.TsSeq, d.URI,
				tt.expectField, tt.expectMergeType, tt.expectTsSeq, tt.expectURI)
		}
	}
}

func TestSnapValue(t *testing.T) {
	stop := TimeSeqID{1700000100, 500}
	uri := "oss://my-bucket/4f3a/(users/1700000100_500.snap"

	val, err := EncodeSnapValue(stop, uri)
	if err != nil {
		t.Fatalf("EncodeSnapValue: %v", err)
	}
	gotStop, gotURI, err := DecodeSnapValue(val)
	if err != nil {
		t.Fatalf("DecodeSnapValue(%q): %v", val, err)
	}
	if gotStop != stop || gotURI != uri {
		t.Errorf("round-trip: got (%v, %q), want (%v, %q)", gotStop, gotURI, stop, uri)
	}

	if !IsDeltaMember(mkMember(2, "/x", "1700000000_1", uri)) {
		t.Error("IsDeltaMember(delta member) = false, want true")
	}
	if IsDeltaMember("snap|whatever") {
		t.Error("IsDeltaMember(non-array) = true, want false")
	}

	for _, c := range []string{"", "notjson", `["bad"]`, `["1700000100_","u"]`, `["1700000100_500",""]`} {
		if _, _, err := DecodeSnapValue(c); err == nil {
			t.Errorf("DecodeSnapValue(%q) expected error, got nil", c)
		}
	}
}
