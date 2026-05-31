package lake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hkloudou/lake/v3/internal/index"
)

func TestFillDeltasBody_CanceledContextDoesNotHang(t *testing.T) {
	c := newTestClient("127.0.0.1:1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.fillDeltasBody(ctx, "users", []index.DeltaInfo{{
			TsSeq: index.TimeSeqID{Timestamp: 1700000000, SeqID: 1},
			URI:   "mem://data/missing.dat",
		}})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("fillDeltasBody hung after context cancellation")
	}
}
