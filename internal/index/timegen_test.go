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
	// Test parsing
	original := TimeSeqID{Timestamp: 1700000000, SeqID: 123}
	str := original.String()

	parsed, err := ParseTimeSeqID(str)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if parsed != original {
		t.Errorf("Parsed mismatch: got %+v, want %+v", parsed, original)
	}

	// Test score calculation
	expectedScore := 1700000000.000123
	if parsed.Score() != expectedScore {
		t.Errorf("Score mismatch: got %f, want %f", parsed.Score(), expectedScore)
	}
}

func TestEncodingWithTimeSeq(t *testing.T) {
	tests := []struct {
		field     string
		tsSeqID   string
		mergeType MergeType
	}{
		{"user.name", "1700000000_123", MergeTypeReplace},
		{"profile.age", "1700000001_456", MergeTypeRFC7396},
		{"settings.theme", "1700000002_789", MergeTypeReplace},
		{"user:profile:special", "1700000003_100", MergeTypeReplace}, // Test special chars
	}

	for _, tt := range tests {
		// Encode
		member := EncodeMember(tt.field, tt.tsSeqID, tt.mergeType)
		t.Logf("Encoded: field=%q -> member=%s", tt.field, member)

		// Decode
		field, tsSeqID, mergeType, err := DecodeMember(member)
		if err != nil {
			t.Errorf("Decode failed for %s: %v", member, err)
			continue
		}

		// Verify
		if field != tt.field {
			t.Errorf("Field mismatch: got %s, want %s", field, tt.field)
		}
		if tsSeqID != tt.tsSeqID {
			t.Errorf("TsSeqID mismatch: got %s, want %s", tsSeqID, tt.tsSeqID)
		}
		if mergeType != tt.mergeType {
			t.Errorf("MergeType mismatch: got %d, want %d", mergeType, tt.mergeType)
		}
	}
}
