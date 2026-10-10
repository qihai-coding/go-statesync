package main

import (
	"testing"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
)

func TestAgeHistogramAndEvidence(t *testing.T) {
	h := ageHistogram{}
	if h.p95() != 0 {
		t.Fatal("empty histogram reported an age")
	}
	for i := 0; i < 95; i++ {
		h.add(200 * time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		h.add(11 * time.Second)
	}
	if h.p95() != .21 || h.maximum != 11*time.Second {
		t.Fatal("percentile did not use conservative bucket bounds")
	}
	overflow := ageHistogram{}
	overflow.add(20 * time.Second)
	if overflow.p95() < 20 {
		t.Fatal("overflow bucket understated display age")
	}
	for _, duration := range []time.Duration{time.Second, 5 * time.Second} {
		out := Report{Workload: "motion", Clients: 1, Entities: 2}
		reportAges(&out, nil, nil, duration)
		if out.DisplayAgeEvaluated || out.DisplayAgePassed {
			t.Fatal("short run claimed display-age acceptance")
		}
	}
	out := Report{Workload: "motion", Clients: 1, Entities: 2}
	reportAges(&out, []EntityAge{{Samples: 100, P95Seconds: .5, MissingSamples: 1}}, nil, 6*time.Second)
	if !out.DisplayAgeEvaluated || out.DisplayAgePassed {
		t.Fatal("missing remote sample passed")
	}
	clear(out.EntityAges)
	reportAges(&out, nil, nil, 6*time.Second)
	if out.DisplayAgePassed {
		t.Fatal("missing entity was omitted from acceptance")
	}
}

func TestApplicationTrafficIncludesRetiredSends(t *testing.T) {
	out := Report{Rooms: []RoomReport{{Metrics: syncnet.Metrics{DatagramBytes: 100, ReliableBytes: 20, ExpectedEntityUpdates: 8, SentEntityUpdates: 7, SnapshotDrops: 1}}}}
	reportTraffic(&out, []clientState{{Sent: 30, TrafficDurationSeconds: 2, EntityUpdates: 6}, {Sent: 40, TrafficDurationSeconds: 3}})
	if out.ServerOutboundBytes != 120 || out.ClientUpstreamBytes != 70 || out.TotalApplicationBytes != 190 || out.TrafficDurationSeconds != 3 || out.AppliedEntityUpdates != 6 {
		t.Fatal("traffic omitted an upstream sender or counted receive bytes", out)
	}
	if out.ExpectedEntityUpdates != 8 || out.SentEntityUpdates != 7 || out.SnapshotDrops != 1 {
		t.Fatal("logical workload counters missing")
	}
}

func TestQueueAndBaselineHardLimits(t *testing.T) {
	m := syncnet.Metrics{InputQueue: 256, ControlQueue: 128, ReliableQueued: 128, SnapshotQueued: 2, BaselineEntities: 16, BaselineBytes: 512}
	if !queuesBounded([]syncnet.Metrics{m}, 2, 8, 32) {
		t.Fatal("exact bounds rejected")
	}
	for _, bad := range []syncnet.Metrics{{InputQueue: 257}, {ControlQueue: 129}, {ReliableQueued: 129}, {SnapshotQueued: 3}, {BaselineEntities: 17}, {BaselineBytes: 513}} {
		if queuesBounded([]syncnet.Metrics{bad}, 2, 8, 32) {
			t.Fatal("resource overflow accepted", bad)
		}
	}
	out := Report{QueuesBounded: true, Entities: 8, StateBytes: 32}
	checkClientBounds(&out, []clientState{{BaselineEntities: 9}})
	if out.QueuesBounded {
		t.Fatal("client dictionary overflow accepted")
	}
}

func TestRecoveryRequiresRemoteDisplayAndPrediction(t *testing.T) {
	local := syncnet.Entity{ID: 1, Generation: 1, Owner: 1, Dynamic: true, State: []byte{1}}
	remote := syncnet.Entity{ID: 2, Generation: 1, Dynamic: true, State: []byte{2}}
	view := syncnet.View{Entities: []syncnet.Entity{local, remote}}
	state := clientState{View: syncnet.View{Player: 1, Entities: view.Entities}, Predicted: []byte{1}, Rendered: map[uint32][]byte{2: {2}}}
	if !converged(view, state) {
		t.Fatal("matching states did not converge")
	}
	state.Rendered[2] = []byte{3}
	if converged(view, state) {
		t.Fatal("stale remote display passed")
	}
	state.Rendered[2] = []byte{2}
	state.Predicted = []byte{3}
	if converged(view, state) {
		t.Fatal("stale prediction passed")
	}
}

func TestMotionRunKeepsShortAgeUnassessed(t *testing.T) {
	o := isolatedOptions{Duration: time.Second, Clients: 2, Rooms: 1, workloadOptions: workloadOptions{Workload: "motion", Entities: 8, StateBytes: 32, DatagramSize: 600, Encoding: "delta", Seed: 7}}
	out, err := runConfigured(o)
	if err != nil || !out.Passed || !out.Converged || !out.RecoveryWithoutReliableCorrection || !out.QueuesBounded {
		t.Fatal("motion smoke failed", err, out.Failures)
	}
	if out.DisplayAgeEvaluated || out.DisplayAgePassed || out.TotalApplicationBytes == 0 || out.ClientUpstreamBytes == 0 {
		t.Fatal("short motion evidence missing or overstated")
	}
}
