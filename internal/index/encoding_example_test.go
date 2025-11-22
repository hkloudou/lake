package index

import (
	"fmt"
)

// Example demonstrates the encoding/decoding with base64 URL encoding
func ExampleEncodeMember() {
	// Example 1: Simple field with Replace merge type
	member := EncodeMember("user.name", "1700000000_123", MergeTypeReplace)
	fmt.Println(member)
	// Output: data|dXNlci5uYW1l|1700000000_123|1
}

func ExampleEncodeMember_specialCharacters() {
	// Example 2: Field with special characters (colons, spaces, etc.)
	member := EncodeMember("user:profile:name with spaces", "1700000000_123", MergeTypeRFC6902)
	fmt.Println(member)
	// Output: data|dXNlcjpwcm9maWxlOm5hbWUgd2l0aCBzcGFjZXM=|1700000000_123|3
}

func ExampleDecodeMember() {
	// Decode a member with Replace merge type
	field, tsSeqID, mergeType, err := DecodeMember("data|dXNlci5uYW1l|1700000000_123|1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Field: %s\n", field)
	fmt.Printf("TsSeqID: %s\n", tsSeqID)
	fmt.Printf("MergeType: %s\n", mergeType.String())
	// Output:
	// Field: user.name
	// TsSeqID: 1700000000_123
	// MergeType: replace
}

func ExampleDecodeMember_specialCharacters() {
	// Decode a member with RFC7396 merge type and special characters
	field, tsSeqID, mergeType, err := DecodeMember("data|dXNlcjpwcm9maWxlOm5hbWUgd2l0aCBzcGFjZXM=|1700000000_123|2")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Field: %s\n", field)
	fmt.Printf("TsSeqID: %s\n", tsSeqID)
	fmt.Printf("MergeType: %s\n", mergeType.String())
	// Output:
	// Field: user:profile:name with spaces
	// TsSeqID: 1700000000_123
	// MergeType: rfc7396
}
