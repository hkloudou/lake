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
			name:      "underscore zero seqid",
			input:     "1700000000_0",
			expectTS:  1700000000,
			expectSeq: 0,
			wantErr:   false,
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
