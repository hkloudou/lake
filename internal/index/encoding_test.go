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
		expectField     string
		expectMergeType MergeType
		expectTsSeq     TimeSeqID
		expectError     bool
	}{
		// Format: delta|{mergeType}|{field}|{tsSeq}
		{"delta|1|/user/name|1700000000_1", "/user/name", MergeTypeReplace, TimeSeqID{1700000000, 1}, false},
		{"delta|2|/profile|1700000000_2", "/profile", MergeTypeRFC7396, TimeSeqID{1700000000, 2}, false},
		{"delta|3|/|1700000000_3", "/", MergeTypeRFC6902, TimeSeqID{1700000000, 3}, false},
		{"delta|1|/user.info|1700000100_123", "/user.info", MergeTypeReplace, TimeSeqID{1700000100, 123}, false},
		// Invalid formats
		{"invalid", "", MergeTypeUnknown, TimeSeqID{}, true},
		{"delta|only", "", MergeTypeUnknown, TimeSeqID{}, true},
		{"data|1|field|1700000000_1", "", MergeTypeUnknown, TimeSeqID{}, true},        // Wrong prefix
		{"delta|1|field", "", MergeTypeUnknown, TimeSeqID{}, true},                    // Too few parts
		{"delta|1|field|1700000000_1|extra", "", MergeTypeUnknown, TimeSeqID{}, true}, // Too many parts
	}

	for _, tt := range tests {
		field, mergeType, tsSeq, err := DecodeDeltaMember(tt.member)
		if tt.expectError {
			if err == nil {
				t.Errorf("DecodeMember(%q) expected error, got nil", tt.member)
			}
		} else {
			if err != nil {
				t.Errorf("DecodeMember(%q) unexpected error: %v", tt.member, err)
			}
			if field != tt.expectField || mergeType != tt.expectMergeType || tsSeq != tt.expectTsSeq {
				t.Errorf("DecodeMember(%q) = (%q, %d, %v), want (%q, %d, %v)",
					tt.member, field, mergeType, tsSeq,
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
