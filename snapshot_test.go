package lake

import (
	"context"
	"testing"

	"github.com/hkloudou/lake/v3/storage"
)

// TestSaveSnapshot_EmitsSnapshotErrorEvent: the snapshot save runs
// fire-and-forget off the read path, so its error never reaches a caller —
// the SnapshotError event is the only way an operator can notice a snap
// target that never works (bad credentials, missing bucket, down store).
func TestSaveSnapshot_EmitsSnapshotErrorEvent(t *testing.T) {
	resolve := func(_ storage.Kind, _, _ string) (storage.Storage, error) {
		return nil, errIntentional // every resolution fails, incl. the snap target
	}
	c := newDeadClientOpts(t, resolve, WithSnapTarget("mem", "snaps"))
	spy := &spyHandler{}
	c.Use(spy.handler())

	_, err := c.saveSnapshot(context.Background(), "users",
		TimeSeqID{Timestamp: 1700000000, SeqID: 1}, "0", []byte("{}"))
	if err == nil {
		t.Fatal("expected saveSnapshot error, got nil")
	}
	if !spy.seen("SnapshotError") {
		t.Fatal("SnapshotError event must be emitted when the snapshot save fails")
	}
}
