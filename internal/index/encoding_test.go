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

func TestSnapMember(t *testing.T) {
	tests := []struct {
		name        string
		startTsSeq  TimeSeqID
		stopTsSeq   TimeSeqID
		expected    string
		expectError bool
	}{
		{"normal", TimeSeqID{1700000000, 1}, TimeSeqID{1700000100, 500}, "snap|1700000000_1|1700000100_500", false},
		{"first snap", TimeSeqID{0, 0}, TimeSeqID{1700000100, 500}, "snap|0_0|1700000100_500", false},
		{"consecutive", TimeSeqID{1700000100, 500}, TimeSeqID{1700000200, 999}, "snap|1700000100_500|1700000200_999", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := EncodeSnapMember(tt.startTsSeq, tt.stopTsSeq)
			if encoded != tt.expected {
				t.Errorf("EncodeSnapMember(%v, %v) = %q, want %q",
					tt.startTsSeq, tt.stopTsSeq, encoded, tt.expected)
			}

			// Check IsSnapMember
			if !IsSnapMember(encoded) {
				t.Errorf("IsSnapMember(%q) = false, want true", encoded)
			}

			// Decode
			decodedStart, decodedStop, err := DecodeSnapMember(encoded)
			if err != nil {
				t.Errorf("DecodeSnapMember(%q) unexpected error: %v", encoded, err)
			}
			if decodedStart != tt.startTsSeq {
				t.Errorf("DecodeSnapMember(%q) start = %v, want %v",
					encoded, decodedStart, tt.startTsSeq)
			}
			if decodedStop != tt.stopTsSeq {
				t.Errorf("DecodeSnapMember(%q) stop = %v, want %v",
					encoded, decodedStop, tt.stopTsSeq)
			}
		})
	}

	// Test non-snap member
	if IsSnapMember("delta|1|/user/name|1700000000_123") {
		t.Error("IsSnapMember(\"delta|1|/user/name|1700000000_123\") = true, want false")
	}

	// Test IsDeltaMember with delta prefix
	if !IsDeltaMember("delta|1|/user/name|1700000000_123") {
		t.Error("IsDeltaMember(\"delta|1|/user/name|1700000000_123\") = false, want true")
	}
	if IsDeltaMember("snap|1700000000_1|1700000100_500") {
		t.Error("IsDeltaMember(\"snap|1700000000_1|1700000100_500\") = true, want false")
	}

	// Test invalid format
	_, _, err := DecodeSnapMember("snap:old-format")
	if err == nil {
		t.Error("DecodeSnapMember(\"snap:old-format\") expected error, got nil")
	}
}
