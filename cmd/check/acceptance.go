package main

import (
	"context"
	"errors"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/arena"
	"github.com/qihai-coding/go-statesync/internal/loadgame"
	"github.com/qihai-coding/go-statesync/internal/measure"
)

type workloadOptions struct {
	Workload, Encoding                 string
	Entities, StateBytes, DatagramSize int
	Seed                               int64
}

func (o workloadOptions) defaults() workloadOptions {
	if o.Workload == "" {
		o.Workload = "arena"
	}
	if o.Encoding == "" {
		o.Encoding = "delta"
	}
	if o.Entities == 0 {
		o.Entities = 256
	}
	if o.StateBytes == 0 {
		o.StateBytes = 32
	}
	if o.DatagramSize == 0 {
		o.DatagramSize = 1000
	}
	return o
}

func (o workloadOptions) validate(players int) error {
	if o.Workload != "arena" && o.Workload != "motion" && o.Workload != "entropy" {
		return errors.New("workload must be arena, motion or entropy")
	}
	if o.Encoding != "delta" && o.Encoding != "full" {
		return errors.New("encoding must be delta or full")
	}
	if o.Entities < players || o.Entities > syncnet.MaxEntities || o.StateBytes < 32 || o.StateBytes > syncnet.MaxState || o.DatagramSize < 600 || o.DatagramSize > 1000 {
		return errors.New("invalid entity, state or datagram size")
	}
	return nil
}

func (o workloadOptions) config() syncnet.Config {
	cfg := syncnet.DefaultConfig()
	cfg.DatagramSize = o.DatagramSize
	if o.Encoding == "full" {
		cfg.SnapshotEncoding = syncnet.FullSnapshots
	}
	return cfg
}

func (o workloadOptions) game(players int) (syncnet.Game, error) {
	if o.Workload == "arena" {
		return arena.New(), nil
	}
	g, err := loadgame.New(o.Entities, players, o.StateBytes)
	if err == nil {
		g.Entropy = o.Workload == "entropy"
	}
	return g, err
}

func (o workloadOptions) model() syncnet.Model {
	if o.Workload == "arena" {
		return arena.Model{}
	}
	return loadgame.Model{StateBytes: o.StateBytes}
}

type roomClock struct {
	Room        int
	Origin      time.Duration
	Uncertainty time.Duration
}

type EntityAge struct {
	Room                                           int
	Player, Entity                                 uint32
	Samples, HeldSamples, MissingSamples           uint64
	P95Seconds, MaxSeconds, SourceUpdateMaxSeconds float64
}

type ageHistogram struct {
	buckets                              [1002]uint32
	n, held, missing                     uint64
	maximum, lastSource, sourceUpdateMax time.Duration
}

func (h *ageHistogram) add(age time.Duration) {
	age = max(0, age)
	h.buckets[min(int(age/(10*time.Millisecond)), 1001)]++
	h.n++
	h.maximum = max(h.maximum, age)
}

func (h *ageHistogram) p95() float64 {
	if h.n == 0 {
		return 0
	}
	want := (h.n*95 + 99) / 100
	var count uint64
	for i, n := range h.buckets {
		count += uint64(n)
		if count >= want {
			if i == len(h.buckets)-1 {
				return float64(h.maximum/(10*time.Millisecond)+1) * .01
			}
			return float64(i+1) * .01
		}
	}
	return 10.02
}

func inspectRooms(ctx context.Context, rooms []*syncnet.Room) ([]syncnet.View, []syncnet.Metrics, error) {
	views, metrics := make([]syncnet.View, len(rooms)), make([]syncnet.Metrics, len(rooms))
	for i, room := range rooms {
		callCtx, cancel := context.WithTimeout(ctx, time.Second)
		view, m, err := room.Inspect(callCtx)
		cancel()
		if err != nil {
			return nil, nil, err
		}
		views[i], metrics[i] = view, m
	}
	return views, metrics, nil
}

func calibrateRooms(ctx context.Context, rooms []*syncnet.Room) ([]roomClock, error) {
	clocks := make([]roomClock, len(rooms))
	for i, room := range rooms {
		best := time.Duration(1<<63 - 1)
		for j := 0; j < 5; j++ {
			before := measure.Now()
			callCtx, cancel := context.WithTimeout(ctx, time.Second)
			view, _, err := room.Inspect(callCtx)
			cancel()
			after := measure.Now()
			if err != nil {
				return nil, err
			}
			if after-before < best {
				best = after - before
				clocks[i] = roomClock{i, before - view.ServerTime, best + time.Microsecond}
			}
		}
	}
	return clocks, nil
}

func reportAges(out *Report, ages []EntityAge, clocks []roomClock, duration time.Duration) {
	out.ClockCalibration = clocks
	out.EntityAges = ages
	out.DisplayAgeEvaluated = out.Workload != "arena" && duration > 5*time.Second
	out.DisplayAgePassed = false
	if !out.DisplayAgeEvaluated {
		return
	}
	out.DisplayAgePassed = true
	if len(ages) != out.Clients*(out.Entities-1) {
		out.DisplayAgePassed = false
	}
	for _, a := range ages {
		out.WorstEntityDisplayAgeP95Seconds = max(out.WorstEntityDisplayAgeP95Seconds, a.P95Seconds)
		if a.Samples == 0 || a.MissingSamples > 0 || a.P95Seconds > 1 {
			out.DisplayAgePassed = false
		}
	}
}

func reportTraffic(out *Report, states []clientState) {
	for _, room := range out.Rooms {
		out.ServerOutboundBytes += room.Metrics.DatagramBytes + room.Metrics.ReliableBytes
		out.ExpectedEntityUpdates += room.Metrics.ExpectedEntityUpdates
		out.SentEntityUpdates += room.Metrics.SentEntityUpdates
		out.SnapshotDrops += room.Metrics.SnapshotDrops
	}
	for _, state := range states {
		out.ClientUpstreamBytes += state.Sent
		out.AppliedEntityUpdates += state.EntityUpdates
		out.TrafficDurationSeconds = max(out.TrafficDurationSeconds, state.TrafficDurationSeconds)
	}
	out.TotalApplicationBytes = out.ServerOutboundBytes + out.ClientUpstreamBytes
	if out.TrafficDurationSeconds > 0 {
		out.ApplicationBytesPerSecond = float64(out.TotalApplicationBytes) / out.TrafficDurationSeconds
	}
	out.BandwidthScope = "all application sends, including initial/resumed full state, lifecycle, inputs, actions and baseline confirmations; excluding QUIC/TLS/UDP/IP overhead and transport retransmissions"
}

func queuesBounded(metrics []syncnet.Metrics, clients, entities, stateBytes int) bool {
	for _, m := range metrics {
		if m.InputQueue > 256 || m.ControlQueue > 128 || m.ReliableQueued > clients*64 || m.SnapshotQueued > clients || m.BaselineEntities > clients*entities || m.BaselineBytes > clients*entities*stateBytes {
			return false
		}
	}
	return true
}

func checkClientBounds(out *Report, states []clientState) {
	for _, state := range states {
		if state.BaselineEntities > out.Entities || state.BaselineBytes > out.Entities*out.StateBytes {
			out.QueuesBounded = false
		}
	}
}
