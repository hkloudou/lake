package index

import "testing"

func TestEncodeMember(t *testing.T) {
	tests := []struct {
		field     string
		mergeType MergeType
		tsSeq     TimeSeqID
		expected  string
	}{
		// Format: delta|{mergeType}|{field}|{tsSeq}
		{"/user/name", MergeTypeReplace, TimeSeqID{1700000000, 1}, "delta|1|/user/name|1700000000_1"},
		{"/profile", MergeTypeRFC7396, TimeSeqID{1700000000, 2}, "delta|2|/profile|1700000000_2"},
		{"/", MergeTypeRFC6902, TimeSeqID{1700000000, 3}, "delta|3|/|1700000000_3"},
		{"/user.info", MergeTypeReplace, TimeSeqID{1700000100, 123}, "delta|1|/user.info|1700000100_123"},
	}

	for _, tt := range tests {
		result := EncodeDeltaMember(tt.field, tt.mergeType, tt.tsSeq)
		if result != tt.expected {
			t.Errorf("EncodeMember(%q, %d, %v) = %q, want %q",
				tt.field, tt.mergeType, tt.tsSeq, result, tt.expected)
		}
	}
}

func TestDecodeMember(t *testing.T) {
	tests := []struct {
		member          string
		score           float64
		expectField     string
		expectMergeType MergeType
		expectTsSeq     TimeSeqID
		expectError     bool
	}{
		// Format: delta|{mergeType}|{field}|{tsSeq}
		{"delta|1|/user/name|1700000000_1", 1700000000.000001, "/user/name", MergeTypeReplace, TimeSeqID{1700000000, 1}, false},
		{"delta|2|/profile|1700000000_2", 1700000000.000002, "/profile", MergeTypeRFC7396, TimeSeqID{1700000000, 2}, false},
		{"delta|3|/|1700000000_3", 1700000000.000003, "/", MergeTypeRFC6902, TimeSeqID{1700000000, 3}, false},
		{"delta|1|/user.info|1700000100_123", 1700000100.000123, "/user.info", MergeTypeReplace, TimeSeqID{1700000100, 123}, false},
		// Invalid formats
		{"invalid", 0, "", MergeTypeUnknown, TimeSeqID{}, true},
		{"delta|only", 0, "", MergeTypeUnknown, TimeSeqID{}, true},
		{"data|1|field|1700000000_1", 0, "", MergeTypeUnknown, TimeSeqID{}, true},        // Wrong prefix
		{"delta|1|field", 0, "", MergeTypeUnknown, TimeSeqID{}, true},                    // Too few parts
		{"delta|1|field|1700000000_1|extra", 0, "", MergeTypeUnknown, TimeSeqID{}, true}, // Too many parts
		// Invalid merge types
		{"delta|0|/user/name|1700000000_1", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true},   // mergeType = 0 (unknown)
		{"delta|4|/user/name|1700000000_1", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true},   // mergeType = 4 (out of range)
		{"delta|-1|/user/name|1700000000_1", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true},  // mergeType = -1 (negative)
		{"delta|999|/user/name|1700000000_1", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true}, // mergeType = 999 (too large)
		{"delta|abc|/user/name|1700000000_1", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true}, // mergeType = non-numeric
		// Invalid field paths
		{"delta|1|user/name|1700000000_1", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true},   // field missing leading /
		{"delta|1|/user/name/|1700000000_1", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true}, // field has trailing /
		{"delta|1|/user|name|1700000000_1", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true},  // field contains | (pipe)
		{"delta|1|/user-name|1700000000_1", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true},  // field contains - (hyphen)
		// Invalid tsSeq
		{"delta|1|/user/name|invalid", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true},            // tsSeq invalid format
		{"delta|1|/user/name|1700000000_01", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true},      // tsSeq with leading zero in seqid
		{"delta|1|/user/name|1700000000_1234567", 1700000000.000001, "", MergeTypeUnknown, TimeSeqID{}, true}, // tsSeq seqid too long
		// Score mismatch
		{"delta|1|/user/name|1700000000_1", 1700000000.000002, "", MergeTypeUnknown, TimeSeqID{}, true}, // tsSeq score doesn't match Redis score
	}

	for _, tt := range tests {
		deltaInfo, err := DecodeDeltaMember(tt.member, tt.score)
		if tt.expectError {
			if err == nil {
				t.Errorf("DecodeMember(%q, %.6f) expected error, got nil", tt.member, tt.score)
			}
		} else {
			if err != nil {
				t.Errorf("DecodeMember(%q, %.6f) unexpected error: %v", tt.member, tt.score, err)
			}
			if deltaInfo.Path != tt.expectField || deltaInfo.MergeType != tt.expectMergeType || deltaInfo.TsSeq != tt.expectTsSeq {
				t.Errorf("DecodeMember(%q, %.6f) = (path=%q, type=%d, tsSeq=%v), want (path=%q, type=%d, tsSeq=%v)",
					tt.member, tt.score, deltaInfo.Path, deltaInfo.MergeType, deltaInfo.TsSeq,
					tt.expectField, tt.expectMergeType, tt.expectTsSeq)
			}
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
			encoded := EncodeSnapValue(tt.stopTsSeq)
			if encoded != tt.expected {
				t.Errorf("EncodeSnapValue(%v) = %q, want %q", tt.stopTsSeq, encoded, tt.expected)
			}

			decodedStop, err := DecodeSnapValue(encoded)
			if err != nil {
				t.Errorf("DecodeSnapValue(%q) unexpected error: %v", encoded, err)
			}
			if decodedStop != tt.stopTsSeq {
				t.Errorf("DecodeSnapValue(%q) stop = %v, want %v",
					encoded, decodedStop, tt.stopTsSeq)
			}
		})
	}

	// IsDeltaMember on the delta zset is unchanged.
	if !IsDeltaMember("delta|1|/user/name|1700000000_123") {
		t.Error("IsDeltaMember(\"delta|1|/user/name|1700000000_123\") = false, want true")
	}

	// Invalid snap value formats
	cases := []string{
		"",                                 // empty
		"snap|1700000000_1|1700000100_500", // legacy "snap|" prefixed format
		"1700000000_1|1700000100_500",      // legacy "start|stop" format
		"badtsseq",                         // unparseable
		"1700000100_",                      // missing seqid
		"_500",                             // missing timestamp
	}
	for _, c := range cases {
		if _, err := DecodeSnapValue(c); err == nil {
			t.Errorf("DecodeSnapValue(%q) expected error, got nil", c)
		}
	}
}
