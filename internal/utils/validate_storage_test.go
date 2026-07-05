package utils

import "testing"

func TestValidateStorageProviderBucket(t *testing.T) {
	valid := []string{"oss", "s3", "cos", "mem", "my-bucket", "b.example", "B_2", "0start"}
	for _, s := range valid {
		if err := ValidateStorageProvider(s); err != nil {
			t.Errorf("provider %q: unexpected error: %v", s, err)
		}
		if err := ValidateStorageBucket(s); err != nil {
			t.Errorf("bucket %q: unexpected error: %v", s, err)
		}
	}

	// "/" and ":" would desynchronise BuildURI/ParseURI; the rest are the
	// usual delimiter / convention hazards shared with catalog names.
	invalid := []string{"", "a/b", "a:b", "a|b", ".hidden", "-flag", "空", "a b", "a\tb"}
	for _, s := range invalid {
		if err := ValidateStorageProvider(s); err == nil {
			t.Errorf("provider %q: expected error, got nil", s)
		}
		if err := ValidateStorageBucket(s); err == nil {
			t.Errorf("bucket %q: expected error, got nil", s)
		}
	}
}
