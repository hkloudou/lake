package index

import "testing"

func TestEncodeMember(t *testing.T) {
	tests := []struct {
		field     string
		tsSeqID   string
		mergeType MergeType
		expected  string
	}{
		// Format: delta|{mergeType}|{field}|{tsSeqID}
		{"/user/name", "1700000000_123", MergeTypeReplace, "delta|1|/user/name|1700000000_123"},
		{"/profile", "1700000001_456", MergeTypeRFC7396, "delta|2|/profile|1700000001_456"},
		{"/", "1700000002_789", MergeTypeRFC6902, "delta|3|/|1700000002_789"},
		{"/user.info", "1700000003_100", MergeTypeReplace, "delta|1|/user.info|1700000003_100"},
	}

	for _, tt := range tests {
		result := EncodeDeltaMember(tt.field, tt.tsSeqID, tt.mergeType)
		if result != tt.expected {
			t.Errorf("EncodeMember(%q, %q, %d) = %q, want %q",
				tt.field, tt.tsSeqID, tt.mergeType, result, tt.expected)
		}
	}
}

func TestDecodeMember(t *testing.T) {
	tests := []struct {
		member          string
		expectField     string
		expectTsSeqID   string
		expectMergeType MergeType
		expectError     bool
	}{
		// Format: delta|{mergeType}|{field}|{tsSeqID}
		{"delta|1|/user/name|1700000000_123", "/user/name", "1700000000_123", MergeTypeReplace, false},
		{"delta|2|/profile|1700000001_456", "/profile", "1700000001_456", MergeTypeRFC7396, false},
		{"delta|3|/|1700000002_789", "/", "1700000002_789", MergeTypeRFC6902, false},
		{"delta|1|/user.info|1700000003_100", "/user.info", "1700000003_100", MergeTypeReplace, false},
		// Invalid formats
		{"invalid", "", "", MergeTypeUnknown, true},
		{"delta|only|two", "", "", MergeTypeUnknown, true},
		{"data|1|field|ts", "", "", MergeTypeUnknown, true}, // Wrong prefix
	}

	for _, tt := range tests {
		field, tsSeqID, mergeType, err := DecodeDeltaMember(tt.member)
		if tt.expectError {
			if err == nil {
				t.Errorf("DecodeMember(%q) expected error, got nil", tt.member)
			}
		} else {
			if err != nil {
				t.Errorf("DecodeMember(%q) unexpected error: %v", tt.member, err)
			}
			if field != tt.expectField || tsSeqID != tt.expectTsSeqID || mergeType != tt.expectMergeType {
				t.Errorf("DecodeMember(%q) = (%q, %q, %d), want (%q, %q, %d)",
					tt.member, field, tsSeqID, mergeType,
					tt.expectField, tt.expectTsSeqID, tt.expectMergeType)
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
