package merge

import (
	"strings"
	"testing"

	"github.com/hkloudou/lake/v3/internal/index"
)

// A single unappliable delta fails the whole merge (it is replayed on every
// read). The error must name the offending delta — tsSeq and uuid — so an
// operator can locate and remove it. This is the only recovery handle today.
func TestMerge_ErrorIdentifiesOffendingDelta(t *testing.T) {
	entries := []index.DeltaInfo{
		{MergeType: index.MergeTypeReplace, Path: "/ok", Body: []byte(`"v"`),
			TsSeq: index.TimeSeqID{Timestamp: 1700000000, SeqID: 1}, UUID: "uuid-good"},
		{MergeType: index.MergeTypeRFC6902, Path: "/", Body: []byte(`[{"op":"remove","path":"/nope"}]`),
			TsSeq: index.TimeSeqID{Timestamp: 1700000005, SeqID: 7}, UUID: "uuid-poison"},
	}
	_, err := Merge([]byte(`{}`), entries)
	if err == nil {
		t.Fatal("expected merge to fail on the unappliable delta")
	}
	msg := err.Error()
	for _, want := range []string{"1700000005_7", "uuid-poison"} {
		if !strings.Contains(msg, want) {
			t.Errorf("merge error must contain %q to be diagnosable; got: %s", want, msg)
		}
	}
}
