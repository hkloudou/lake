package lake

import "testing"

// TestSampleFlightIsDistinctFromSnapFlight makes sure sampleFlight and
// snapFlight are independent SingleFlight instances. They previously shared
// one instance via field reuse, with the only thing preventing key
// collision being a "sample:" prefix in the key string. Splitting them
// removes that fragile coupling and removes contention on a single mutex.
func TestSampleFlightIsDistinctFromSnapFlight(t *testing.T) {
	c := newTestClient("127.0.0.1:1") // unreachable; we never call methods that need Redis

	if c.snapFlight == nil || c.sampleFlight == nil {
		t.Fatalf("both flights must be initialized; snap=%v sample=%v", c.snapFlight, c.sampleFlight)
	}
	// Interface equality compares (dynamic type, value pointer); two distinct
	// SingleFlight instances must not compare equal.
	if c.snapFlight == c.sampleFlight {
		t.Fatal("snapFlight and sampleFlight share the same SingleFlight instance; they must be independent")
	}
}
