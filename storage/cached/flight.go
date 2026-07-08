package cached

import (
	"context"
	"errors"

	"github.com/hkloudou/lake/v3/internal/xsync"
)

// takeWithRetry is the shared miss-path harness of every Cache.Take: run
// fill under the single-flight, retry ONCE when the flight failed with the
// LEADER's context cancellation while this caller's own context is healthy
// (a cancelled leader must not poison its whole cohort), and hand every
// caller a private copy — flight waiters would otherwise all alias the
// leader's slice, and Lake explicitly lets callers mutate the documents
// built from these bytes.
//
// fill receives retry=true on the second attempt, so it can re-check the
// cache before hitting the loader again.
func takeWithRetry(ctx context.Context, flight xsync.SingleFlight[[]byte], key string, fill func(retry bool) ([]byte, error)) ([]byte, error) {
	v, err := flight.Do(key, func() ([]byte, error) { return fill(false) })
	if err != nil && ctx.Err() == nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		v, err = flight.Do(key, func() ([]byte, error) { return fill(true) })
	}
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), v...), nil
}
