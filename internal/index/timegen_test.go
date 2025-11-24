package index

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestTimeGenerator(t *testing.T) {
	// Connect to local Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // Use test DB
	})
	defer rdb.Close()

	ctx := context.Background()

	// Ping Redis
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	gen := NewTimeGenerator(rdb)

	// Test generating multiple TimeSeqIDs
	ids := make([]TimeSeqID, 10)
	for i := 0; i < 10; i++ {
		tsSeq, err := gen.Generate(ctx, "test")
		if err != nil {
			t.Fatalf("Generate failed: %v", err)
		}
		ids[i] = tsSeq

		t.Logf("Generated: ts=%d, seqid=%d, score=%f, string=%s",
			tsSeq.Timestamp, tsSeq.SeqID, tsSeq.Score(), tsSeq.String())
	}

	// Verify all IDs are unique
	seen := make(map[string]bool)
	for _, id := range ids {
		key := id.String()
		if seen[key] {
			t.Errorf("Duplicate ID: %s", key)
		}
		seen[key] = true
	}

	// Verify scores are monotonically increasing (within same second)
	for i := 1; i < len(ids); i++ {
		if ids[i].Timestamp == ids[i-1].Timestamp {
			if ids[i].Score() <= ids[i-1].Score() {
				t.Errorf("Score not increasing: %f <= %f", ids[i].Score(), ids[i-1].Score())
			}
		}
	}
}

func TestTimeGeneratorCatalogIsolation(t *testing.T) {
	// Connect to local Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // Use test DB
	})
	defer rdb.Close()

	ctx := context.Background()

	// Ping Redis
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	gen := NewTimeGenerator(rdb)

	// Generate seqids for different catalogs
	catalog1 := "users"
	catalog2 := "products"

	// Generate 5 IDs for catalog1
	catalog1IDs := make([]TimeSeqID, 5)
	for i := 0; i < 5; i++ {
		tsSeq, err := gen.Generate(ctx, catalog1)
		if err != nil {
			t.Fatalf("Generate failed for %s: %v", catalog1, err)
		}
		catalog1IDs[i] = tsSeq
		t.Logf("[%s] Generated: ts=%d, seqid=%d", catalog1, tsSeq.Timestamp, tsSeq.SeqID)
	}

	// Generate 5 IDs for catalog2
	catalog2IDs := make([]TimeSeqID, 5)
	for i := 0; i < 5; i++ {
		tsSeq, err := gen.Generate(ctx, catalog2)
		if err != nil {
			t.Fatalf("Generate failed for %s: %v", catalog2, err)
		}
		catalog2IDs[i] = tsSeq
		t.Logf("[%s] Generated: ts=%d, seqid=%d", catalog2, tsSeq.Timestamp, tsSeq.SeqID)
	}

	// Verify: if within same second, catalog1 and catalog2 should have independent seqids
	// Both should start from seqid=1 within their own catalog
	for i, id1 := range catalog1IDs {
		for _, id2 := range catalog2IDs {
			if id1.Timestamp == id2.Timestamp {
				t.Logf("Same timestamp=%d: catalog1.seqid=%d, catalog2.seqid=%d",
					id1.Timestamp, id1.SeqID, id2.SeqID)
				// They are isolated, so seqid can be the same or different
				// Just verify both sequences are independent and monotonic
			}
		}
		// Verify monotonic within same catalog
		if i > 0 && catalog1IDs[i].Timestamp == catalog1IDs[i-1].Timestamp {
			if catalog1IDs[i].SeqID <= catalog1IDs[i-1].SeqID {
				t.Errorf("[%s] SeqID not increasing: %d <= %d",
					catalog1, catalog1IDs[i].SeqID, catalog1IDs[i-1].SeqID)
			}
		}
	}
}

func TestTimeSeqIDParsing(t *testing.T) {
	tests := []struct {
		name      string
		input     any
		expectTS  int64
		expectSeq int64
		wantErr   bool
	}{
		// String format: "timestamp_seqid"
		{
			name:      "string underscore format",
			input:     "1700000000_123",
			expectTS:  1700000000,
			expectSeq: 123,
			wantErr:   false,
		},
		{
			name:      "string underscore zero seqid",
			input:     "1700000000_0",
			expectTS:  1700000000,
			expectSeq: 0,
			wantErr:   false,
		},
		{
			name:      "string underscore max seqid",
			input:     "1700000000_999999",
			expectTS:  1700000000,
			expectSeq: 999999,
			wantErr:   false,
		},

		// String format: "timestamp.seqid" (decimal with 1-6 digits, seqid > 0)
		{
			name:      "string decimal 6 digits",
			input:     "1700000000.000123",
			expectTS:  1700000000,
			expectSeq: 123,
			wantErr:   false,
		},
		{
			name:      "string decimal 1 digit",
			input:     "1700000000.1",
			expectTS:  1700000000,
			expectSeq: 100000, // Auto-padded to 6 digits
			wantErr:   false,
		},
		{
			name:      "string decimal 2 digits",
			input:     "1700000000.12",
			expectTS:  1700000000,
			expectSeq: 120000,
			wantErr:   false,
		},
		{
			name:      "string decimal 6 digits - max",
			input:     "1700000000.123456",
			expectTS:  1700000000,
			expectSeq: 123456,
			wantErr:   false,
		},
		{
			name:    "string decimal all zeros - invalid",
			input:   "1700000000.000000",
			wantErr: true, // seqid cannot be 0
		},
		{
			name:    "string decimal 1 digit zero - invalid",
			input:   "1700000000.0",
			wantErr: true, // seqid cannot be 0
		},
		{
			name:    "string no decimal - invalid",
			input:   "1700000000",
			wantErr: true, // Must have decimal point
		},

		// Float64 format (seqid > 0)
		{
			name:      "float64 format - 6 digits",
			input:     1700000000.000123,
			expectTS:  1700000000,
			expectSeq: 123,
			wantErr:   false,
		},
		{
			name:      "float64 1 digit",
			input:     1700000000.1,
			expectTS:  1700000000,
			expectSeq: 100000,
			wantErr:   false,
		},
		{
			name:      "float64 6 digits precision - max",
			input:     1700000000.123456,
			expectTS:  1700000000,
			expectSeq: 123456,
			wantErr:   false,
		},
		{
			name:    "float64 all zeros - invalid",
			input:   1700000000.000000,
			wantErr: true, // seqid cannot be 0
		},

		// Error cases
		{
			name:    "invalid string format",
			input:   "invalid",
			wantErr: true,
		},
		{
			name:    "unsupported type",
			input:   123,
			wantErr: true,
		},
		{
			name:      "string decimal min valid - 0.000001",
			input:     "1700000000.000001",
			expectTS:  1700000000,
			expectSeq: 1,
			wantErr:   false,
		},
		{
			name:      "string decimal 5 digits",
			input:     "1700000000.00001",
			expectTS:  1700000000,
			expectSeq: 10,
			wantErr:   false,
		},
		{
			name:    "string decimal 7 digits - invalid",
			input:   "1700000000.0000005",
			wantErr: true, // More than 6 digits
		},
		{
			name:    "string decimal 8 digits - invalid",
			input:   "1700000000.00000001",
			wantErr: true, // More than 6 digits
		},
		{
			name:    "float64 7 decimals - 0.0000001 - invalid",
			input:   1700000000.0000001,
			wantErr: true, // More than 6 decimal places
		},
		{
			name:    "float64 7 decimals - 0.0000005 - invalid",
			input:   1700000000.0000005,
			wantErr: true, // More than 6 decimal places
		},
		{
			name:    "float64 7 decimals - 0.1234567 - invalid",
			input:   1700000000.1234567,
			wantErr: true, // More than 6 decimal places
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseTimeSeqID(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseTimeSeqID(%v) expected error, got nil", tt.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("ParseTimeSeqID(%v) unexpected error: %v", tt.input, err)
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
	t.Run("round-trip string format", func(t *testing.T) {
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

	// Test round-trip: TimeSeqID -> Score -> ParseTimeSeqID
	t.Run("round-trip score format", func(t *testing.T) {
		original := TimeSeqID{Timestamp: 1700000000, SeqID: 123}
		score := original.Score()

		parsed, err := ParseTimeSeqID(score)
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if parsed != original {
			t.Errorf("Round-trip mismatch: got %+v, want %+v", parsed, original)
		}
	})
}
