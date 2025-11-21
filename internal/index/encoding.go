package index

import (
	"fmt"
	"strings"
)

// EncodeMember encodes field and uuid into Redis ZADD member format: "field:uuid"
func EncodeMember(field, uuid string) string {
	return fmt.Sprintf("%s:%s", field, uuid)
}

// DecodeMember decodes Redis ZADD member into field and uuid
func DecodeMember(member string) (field, uuid string, err error) {
	parts := strings.SplitN(member, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid member format: %s", member)
	}
	return parts[0], parts[1], nil
}

// EncodeSnapMember encodes snapshot uuid into Redis ZADD member format: "snap:uuid"
func EncodeSnapMember(uuid string) string {
	return fmt.Sprintf("snap:%s", uuid)
}

// DecodeSnapMember decodes snapshot member and returns uuid
func DecodeSnapMember(member string) (uuid string, err error) {
	if !strings.HasPrefix(member, "snap:") {
		return "", fmt.Errorf("invalid snap member format: %s", member)
	}
	return strings.TrimPrefix(member, "snap:"), nil
}

// IsSnapMember checks if member is a snapshot member
func IsSnapMember(member string) bool {
	return strings.HasPrefix(member, "snap:")
}
