package statesync

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"
)

func checkSnapshotAck(t *testing.T, tick, ack uint64, value byte) {
	t.Helper()
	c := pendingResyncClient(t)
	c.resyncing = false
	c.lastResync = time.Now()
	c.entities[c.local].tick = 10
	before := c.Authoritative()
	history := append([]Input(nil), c.history...)
	samples := append([]sample(nil), c.entities[c.local].samples...)
	anchor, anchorAt := c.anchor, c.anchorAt
	e := c.entities[c.local].entity
	e.Ack, e.State = ack, []byte{value}
	packets, err := snapshotPackets(tick, c.revision, []Entity{e}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	err = c.applySnapshot(packets[0])
	invalid := tick > 10 && (ack < 2 || ack > 5)
	if invalid != errors.Is(err, ErrProtocol) || !invalid && err != nil {
		t.Fatalf("tick=%d ack=%d: unexpected result %v", tick, ack, err)
	}
	if invalid || tick <= 10 {
		if !reflect.DeepEqual(before, c.Authoritative()) || !reflect.DeepEqual(history, c.history) ||
			!reflect.DeepEqual(samples, c.entities[c.local].samples) || !bytes.Equal(c.predicted, []byte{3}) ||
			c.anchor != anchor || c.anchorAt != anchorAt || c.resyncing {
			t.Fatal("rejected or stale snapshot changed state, prediction or clock")
		}
	} else if c.entities[c.local].entity.Ack != ack || len(c.history) != int(5-ack) ||
		c.predicted[0] != value+byte(5-ack) || c.tick != tick {
		t.Fatal("accepted snapshot failed to reconcile pending input")
	}
}

func TestSnapshotAckBounds(t *testing.T) {
	for _, tick := range []uint64{9, 10, 11} {
		for _, ack := range []uint64{0, 1, 2, 3, 5, 6, ^uint64(0)} {
			checkSnapshotAck(t, tick, ack, 9)
		}
	}
}

func FuzzSnapshotAck(f *testing.F) {
	for _, tick := range []uint64{9, 10, 11} {
		for _, ack := range []uint64{1, 2, 5, 6, ^uint64(0)} {
			f.Add(tick, ack, byte(9))
		}
	}
	f.Fuzz(checkSnapshotAck)
}
