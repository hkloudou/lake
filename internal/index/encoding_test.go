package index

import "testing"

const testUUID = "0123456789abcdef0123456789abcdef"

func TestEncodeMember(t *testing.T) {
	tests := []struct {
		field     string
		mergeType MergeType
		tsSeq     TimeSeqID
		expected  string
	}{
		{"/user/name", MergeTypeReplace, TimeSeqID{1700000000, 1}, "delta|1|/user/name|1700000000_1|" + testUUID},
		{"/profile", MergeTypeRFC7396, TimeSeqID{1700000000, 2}, "delta|2|/profile|1700000000_2|" + testUUID},
		{"/", MergeTypeRFC6902, TimeSeqID{1700000000, 3}, "delta|3|/|1700000000_3|" + testUUID},
		{"/user.info", MergeTypeReplace, TimeSeqID{1700000100, 123}, "delta|1|/user.info|1700000100_123|" + testUUID},
	}
	for _, tt := range tests {
		got := EncodeDeltaMember(tt.field, tt.mergeType, tt.tsSeq, testUUID)
		if got != tt.expected {
			t.Errorf("EncodeDeltaMember(%q, %d, %v, uuid) = %q, want %q",
				tt.field, tt.mergeType, tt.tsSeq, got, tt.expected)
		}
	}
}

func TestDecodeMember(t *testing.T) {
	const u = testUUID
	tests := []struct {
		member          string
		score           float64
		expectField     string
		expectMergeType MergeType
		expectTsSeq     TimeSeqID
		expectUUID      string
		expectError     bool
	}{
		{"delta|1|/user/name|1700000000_1|" + u, 1700000000.000001, "/user/name", MergeTypeReplace, TimeSeqID{1700000000, 1}, u, false},
		{"delta|2|/profile|1700000000_2|" + u, 1700000000.000002, "/profile", MergeTypeRFC7396, TimeSeqID{1700000000, 2}, u, false},
		{"delta|3|/|1700000000_3|" + u, 1700000000.000003, "/", MergeTypeRFC6902, TimeSeqID{1700000000, 3}, u, false},
		// Invalid formats
		{"invalid", 0, "", 0, TimeSeqID{}, "", true},
		{"delta|only", 0, "", 0, TimeSeqID{}, "", true},
		{"data|1|field|1700000000_1|" + u, 0, "", 0, TimeSeqID{}, "", true},                // wrong prefix
		{"delta|1|/x|1700000000_1", 0, "", 0, TimeSeqID{}, "", true},                       // 4 parts (legacy)
		{"delta|1|/x|1700000000_1|" + u + "|extra", 0, "", 0, TimeSeqID{}, "", true},       // 6 parts
		{"delta|1|/x|1700000000_1|", 1700000000.000001, "", 0, TimeSeqID{}, "", true},      // empty uuid
		{"delta|0|/user/name|1700000000_1|" + u, 1700000000.000001, "", 0, TimeSeqID{}, "", true},
		{"delta|4|/user/name|1700000000_1|" + u, 1700000000.000001, "", 0, TimeSeqID{}, "", true},
		{"delta|abc|/user/name|1700000000_1|" + u, 1700000000.000001, "", 0, TimeSeqID{}, "", true},
		{"delta|1|user/name|1700000000_1|" + u, 1700000000.000001, "", 0, TimeSeqID{}, "", true},
		{"delta|1|/user/name|invalid|" + u, 1700000000.000001, "", 0, TimeSeqID{}, "", true},
		{"delta|1|/user/name|1700000000_1|" + u, 1700000000.000002, "", 0, TimeSeqID{}, "", true}, // score mismatch
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
		if d.Path != tt.expectField || d.MergeType != tt.expectMergeType || d.TsSeq != tt.expectTsSeq || d.UUID != tt.expectUUID {
			t.Errorf("DecodeDeltaMember(%q) = (path=%q, type=%d, tsSeq=%v, uuid=%q), want (path=%q, type=%d, tsSeq=%v, uuid=%q)",
				tt.member, d.Path, d.MergeType, d.TsSeq, d.UUID,
				tt.expectField, tt.expectMergeType, tt.expectTsSeq, tt.expectUUID)
		}
	}
}

func TestSnapValue(t *testing.T) {
	tests := []struct {
		name      string
		stopTsSeq TimeSeqID
		expected  string
	}{
		{"normal", TimeSeqID{1700000100, 500}, "1700000100_500"},
		{"high seqid", TimeSeqID{1700000200, 999}, "1700000200_999"},
		{"unit seqid", TimeSeqID{1700000300, 1}, "1700000300_1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EncodeSnapValue(tt.stopTsSeq)
			if got != tt.expected {
				t.Errorf("EncodeSnapValue(%v) = %q, want %q", tt.stopTsSeq, got, tt.expected)
			}
			stop, err := DecodeSnapValue(got)
			if err != nil {
				t.Errorf("DecodeSnapValue(%q) unexpected error: %v", got, err)
			}
			if stop != tt.stopTsSeq {
				t.Errorf("DecodeSnapValue(%q) = %v, want %v", got, stop, tt.stopTsSeq)
			}
		})
	}

	if !IsDeltaMember("delta|1|/user/name|1700000000_123|" + testUUID) {
		t.Error("IsDeltaMember(...) = false, want true")
	}

	for _, c := range []string{"", "snap|1700000000_1|1700000100_500", "1700000000_1|1700000100_500", "badtsseq", "1700000100_", "_500"} {
		if _, err := DecodeSnapValue(c); err == nil {
			t.Errorf("DecodeSnapValue(%q) expected error, got nil", c)
		}
	}
}
