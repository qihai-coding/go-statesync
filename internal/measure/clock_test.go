package measure

import (
	"testing"
	"time"
)

func TestClockResolvesShortIntervals(t *testing.T) {
	previous := Now()
	smallest := time.Hour
	for range 10000 {
		current := Now()
		if current < previous {
			t.Fatal("clock moved backwards")
		}
		if current > previous {
			smallest = min(smallest, current-previous)
		}
		previous = current
	}
	if smallest > time.Microsecond {
		t.Fatalf("clock resolution too coarse: %v", smallest)
	}
	t.Logf("smallest positive clock interval: %v", smallest)
}
