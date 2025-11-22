package index

import "testing"

func TestEncodeMember(t *testing.T) {
	tests := []struct {
		field     string
		tsSeqID   string
		mergeType MergeType
		expected  string
	}{
		// "user.name" in base64 URL encoding = "dXNlci5uYW1l"
		{"user.name", "1700000000_123", MergeTypeReplace, "delta|dXNlci5uYW1l|1700000000_123|1"},
		// "profile" in base64 URL encoding (no padding) = "cHJvZmlsZQ"
		{"profile", "1700000001_456", MergeTypeRFC7396, "delta|cHJvZmlsZQ|1700000001_456|2"},
		// "" in base64 URL encoding = ""
		{"", "1700000002_789", MergeTypeRFC6902, "delta||1700000002_789|3"},
		// Test field with special chars: "user:profile"
		{"user:profile", "1700000003_100", MergeTypeReplace, "delta|dXNlcjpwcm9maWxl|1700000003_100|1"},
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
		{"delta|dXNlci5uYW1l|1700000000_123|1", "user.name", "1700000000_123", MergeTypeReplace, false},
		{"delta|cHJvZmlsZQ|1700000001_456|2", "profile", "1700000001_456", MergeTypeRFC7396, false},
		{"delta||1700000002_789|3", "", "1700000002_789", MergeTypeRFC6902, false},
		{"delta|dXNlcjpwcm9maWxl|1700000003_100|1", "user:profile", "1700000003_100", MergeTypeReplace, false},
		{"invalid", "", "", MergeTypeReplace, true},
		{"data:user.name:1700000000_123_0", "", "", MergeTypeReplace, true},      // Old format, should fail
		{"data|invalid-base64|1700000000_123|0", "", "", MergeTypeReplace, true}, // Invalid base64
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
	if IsSnapMember("data|dXNlci5uYW1l|1700000000_123|0") {
		t.Error("IsSnapMember(\"data|dXNlci5uYW1l|1700000000_123|0\") = true, want false")
	}

	// Test IsDataMember
	if !IsDeltaMember("data|dXNlci5uYW1l|1700000000_123|0") {
		t.Error("IsDataMember(\"data|dXNlci5uYW1l|1700000000_123|0\") = false, want true")
	}
	if IsDeltaMember("snap|1700000000_1|1700000100_500") {
		t.Error("IsDataMember(\"snap|1700000000_1|1700000100_500\") = true, want false")
	}

	// Test invalid format
	_, _, err := DecodeSnapMember("snap:old-format")
	if err == nil {
		t.Error("DecodeSnapMember(\"snap:old-format\") expected error, got nil")
	}
}
