package index

import (
	"testing"
)

func TestTimeSeqIDParsing(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectTS  int64
		expectSeq int64
		wantErr   bool
	}{
		// Format: "timestamp_seqid"
		{
			name:      "underscore format",
			input:     "1700000000_123",
			expectTS:  1700000000,
			expectSeq: 123,
			wantErr:   false,
		},
		{
			name:    "underscore zero seqid - invalid",
			input:   "1700000000_0",
			wantErr: true, // seqid "0" starts with 0, only "0_0" is allowed
		},
		{
			name:      "underscore max seqid",
			input:     "1700000000_999999",
			expectTS:  1700000000,
			expectSeq: 999999,
			wantErr:   false,
		},
		{
			name:      "underscore min seqid",
			input:     "1700000000_1",
			expectTS:  1700000000,
			expectSeq: 1,
			wantErr:   false,
		},

		// Error cases
		{
			name:    "invalid string format",
			input:   "invalid",
			wantErr: true,
		},
		{
			name:    "no underscore",
			input:   "1700000000",
			wantErr: true,
		},
		{
			name:    "decimal format not supported",
			input:   "1700000000.000123",
			wantErr: true,
		},
		{
			name:    "missing seqid",
			input:   "1700000000_",
			wantErr: true,
		},
		{
			name:    "missing timestamp",
			input:   "_123",
			wantErr: true,
		},
		{
			name:    "non-numeric timestamp",
			input:   "abc_123",
			wantErr: true,
		},
		{
			name:    "non-numeric seqid",
			input:   "1700000000_abc",
			wantErr: true,
		},
		{
			name:    "seqid with leading zero",
			input:   "1700000000_01",
			wantErr: true,
		},
		{
			name:    "seqid with multiple leading zeros",
			input:   "1700000000_001",
			wantErr: true,
		},
		{
			name:    "seqid too long - 7 digits",
			input:   "1700000000_1234567",
			wantErr: true,
		},
		{
			name:    "seqid too long - 8 digits",
			input:   "1700000000_12345678",
			wantErr: true,
		},
		{
			name:    "timestamp with leading zero",
			input:   "01700000000_123",
			wantErr: true,
		},
		{
			name:    "timestamp with multiple leading zeros",
			input:   "001700000000_123",
			wantErr: true,
		},
		{
			name:      "0_0 is valid - initial snapshot marker",
			input:     "0_0",
			expectTS:  0,
			expectSeq: 0,
			wantErr:   false,
		},
		{
			name:    "timestamp too large - beyond year 3000",
			input:   "99999999999_1",
			wantErr: true,
		},
		{
			name:    "negative timestamp",
			input:   "-1_1",
			wantErr: true,
		},
		{
			name:    "negative seqid",
			input:   "1700000000_-1",
			wantErr: true,
		},
		{
			name:    "timestamp with scientific notation - lowercase e",
			input:   "1.7e9_123",
			wantErr: true,
		},
		{
			name:    "timestamp with scientific notation - uppercase E",
			input:   "1.7E9_123",
			wantErr: true,
		},
		{
			name:    "seqid with scientific notation",
			input:   "1700000000_1e2",
			wantErr: true,
		},
		{
			name:    "seqid exceeds max - 1000000",
			input:   "1700000000_1000000",
			wantErr: true,
		},
		{
			name:    "seqid exceeds max - 9999999",
			input:   "1700000000_9999999",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseTimeSeqID(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseTimeSeqID(%q) expected error, got nil", tt.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("ParseTimeSeqID(%q) unexpected error: %v", tt.input, err)
			}

			if parsed.Timestamp != tt.expectTS {
				t.Errorf("Timestamp mismatch: got %d, want %d", parsed.Timestamp, tt.expectTS)
			}
			if parsed.SeqID != tt.expectSeq {
				t.Errorf("SeqID mismatch: got %d, want %d", parsed.SeqID, tt.expectSeq)
			}

			// Verify score calculation
			expectedScore := float64(tt.expectTS) + float64(tt.expectSeq)/1000000.0
			if parsed.Score() != expectedScore {
				t.Errorf("Score mismatch: got %f, want %f", parsed.Score(), expectedScore)
			}
		})
	}

	// Test round-trip: TimeSeqID -> String -> ParseTimeSeqID
	t.Run("round-trip", func(t *testing.T) {
		original := TimeSeqID{Timestamp: 1700000000, SeqID: 123}
		str := original.String()

		parsed, err := ParseTimeSeqID(str)
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if parsed != original {
			t.Errorf("Round-trip mismatch: got %+v, want %+v", parsed, original)
		}
	})
}
