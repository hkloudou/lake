package index

import "testing"

func TestEncodeMember(t *testing.T) {
	tests := []struct {
		field    string
		uuid     string
		expected string
	}{
		{"user.name", "abc123", "user.name:abc123"},
		{"profile", "def456", "profile:def456"},
		{"", "empty", ":empty"},
	}

	for _, tt := range tests {
		result := EncodeMember(tt.field, tt.uuid)
		if result != tt.expected {
			t.Errorf("EncodeMember(%q, %q) = %q, want %q", tt.field, tt.uuid, result, tt.expected)
		}
	}
}

func TestDecodeMember(t *testing.T) {
	tests := []struct {
		member      string
		expectField string
		expectUUID  string
		expectError bool
	}{
		{"user.name:abc123", "user.name", "abc123", false},
		{"profile:def456", "profile", "def456", false},
		{"invalid", "", "", true},
		{":empty", "", "empty", false},
	}

	for _, tt := range tests {
		field, uuid, err := DecodeMember(tt.member)
		if tt.expectError {
			if err == nil {
				t.Errorf("DecodeMember(%q) expected error, got nil", tt.member)
			}
		} else {
			if err != nil {
				t.Errorf("DecodeMember(%q) unexpected error: %v", tt.member, err)
			}
			if field != tt.expectField || uuid != tt.expectUUID {
				t.Errorf("DecodeMember(%q) = (%q, %q), want (%q, %q)",
					tt.member, field, uuid, tt.expectField, tt.expectUUID)
			}
		}
	}
}

func TestSnapMember(t *testing.T) {
	uuid := "snap-uuid-123"
	encoded := EncodeSnapMember(uuid)
	if encoded != "snap:snap-uuid-123" {
		t.Errorf("EncodeSnapMember(%q) = %q, want %q", uuid, encoded, "snap:snap-uuid-123")
	}

	if !IsSnapMember(encoded) {
		t.Errorf("IsSnapMember(%q) = false, want true", encoded)
	}

	decoded, err := DecodeSnapMember(encoded)
	if err != nil {
		t.Errorf("DecodeSnapMember(%q) unexpected error: %v", encoded, err)
	}
	if decoded != uuid {
		t.Errorf("DecodeSnapMember(%q) = %q, want %q", encoded, decoded, uuid)
	}

	// Test non-snap member
	if IsSnapMember("user.name:abc123") {
		t.Error("IsSnapMember(\"user.name:abc123\") = true, want false")
	}
}
