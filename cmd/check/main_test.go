package main

import (
	"math"
	"testing"
	"time"

	"github.com/qihai-coding/go-statesync/internal/nettest"
)

func TestShortRunDoesNotClaimMemoryStability(t *testing.T) {
	report, err := run(time.Second, 1, 1, 0, nettest.Profile{})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed || !report.Converged || !report.ConnectionsAlive {
		t.Fatal("short functional run failed", report.Failures)
	}
	if report.MemoryChecked || report.MemoryBounded {
		t.Fatal("short run falsely claimed long-term memory stability")
	}
}

func TestRunRejectsNonFiniteProfile(t *testing.T) {
	for _, profile := range []nettest.Profile{{Loss: math.NaN()}, {Duplicate: math.NaN()}, {Reorder: math.NaN()}} {
		if _, err := run(time.Second, 1, 1, 0, profile); err == nil {
			t.Fatal("invalid profile started a workload")
		}
	}
}

func TestMemoryEvidenceAndThresholds(t *testing.T) {
	const baselineHeap = 32 << 20
	const allowedHeap = baselineHeap + baselineHeap/4 + 4*(1<<20)
	for _, tc := range []struct {
		name     string
		duration time.Duration
		samples  []Sample
		checked  bool
		bounded  bool
	}{
		{"no samples", 10 * time.Minute, nil, false, false},
		{"two samples", 10 * time.Minute, []Sample{{Seconds: 60}, {Seconds: 601}}, false, false},
		{"short run", time.Minute, []Sample{{Seconds: 30}, {Seconds: 60}, {Seconds: 61}}, false, false},
		{"early baseline", 10 * time.Minute, []Sample{{Seconds: 1}, {Seconds: 2}, {Seconds: 601}}, false, false},
		{"missing endpoint", 10 * time.Minute, []Sample{{Seconds: 30}, {Seconds: 60}, {Seconds: 599}}, false, false},
		{"reversed time", 10 * time.Minute, []Sample{{Seconds: 30}, {Seconds: 602}, {Seconds: 601}}, false, false},
		{"exact limits", 10 * time.Minute, []Sample{{Seconds: 30}, {Seconds: 60, HeapBytes: baselineHeap, Goroutines: 100}, {Seconds: 601, HeapBytes: allowedHeap, Goroutines: 228}}, true, true},
		{"heap growth", 10 * time.Minute, []Sample{{Seconds: 30}, {Seconds: 60, HeapBytes: baselineHeap, Goroutines: 100}, {Seconds: 601, HeapBytes: allowedHeap + 1, Goroutines: 100}}, true, false},
		{"task growth", 10 * time.Minute, []Sample{{Seconds: 30}, {Seconds: 60, HeapBytes: baselineHeap, Goroutines: 100}, {Seconds: 601, HeapBytes: baselineHeap, Goroutines: 229}}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checked, bounded := checkMemory(tc.duration, tc.samples, 8)
			if checked != tc.checked || bounded != tc.bounded {
				t.Fatalf("checked=%v, bounded=%v; want %v, %v", checked, bounded, tc.checked, tc.bounded)
			}
		})
	}
}
