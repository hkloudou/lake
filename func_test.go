package lake

import "testing"

func TestGetSliceNumericPartOutOfRange(t *testing.T) {
	if got := getSliceNumericPart([]string{"1", "2"}, 5); got != 0 {
		t.Fatalf("expected 0 for out-of-range index, got %d", got)
	}
}
