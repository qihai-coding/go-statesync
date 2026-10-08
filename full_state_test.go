package statesync

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"
)

func pendingResyncClient(t *testing.T) *Client {
	t.Helper()
	c := testClient(t)
	c.token = ResumeToken{1}
	c.lastAction = 42
	c.tick = 10
	c.sequence = 5
	c.entities[c.local].entity.Ack = 2
	c.history = []Input{{3, []byte{1}}, {4, []byte{1}}, {5, []byte{1}}}
	c.recent = append([]Input(nil), c.history...)
	c.predicted = []byte{3}
	c.resyncing = true
	c.lastResync = time.Now().Add(-2 * time.Second)
	return c
}

func TestFullStateRejectsInvalidResync(t *testing.T) {
	for _, reason := range []string{
		"unsolicited", "controlled ID", "controlled generation", "ack behind cutoff", "ack ahead of cutoff",
		"action rollback", "tick rate", "snapshot rate", "grace period", "invalid state",
	} {
		t.Run(reason, func(t *testing.T) {
			c := pendingResyncClient(t)
			v, cfg, session := c.Authoritative(), c.cfg, sessionState{c.token, c.lastAction}
			v.Tick++
			v.Entities[0].Ack = c.sequence
			v.Entities[0].State = []byte{9}
			switch reason {
			case "unsolicited":
				c.resyncing = false
			case "controlled ID":
				v.Entities[0].ID = 3
			case "controlled generation":
				v.Entities[0].Generation++
			case "ack behind cutoff":
				v.Entities[0].Ack--
			case "ack ahead of cutoff":
				v.Entities[0].Ack++
			case "action rollback":
				session.lastAction--
			case "tick rate":
				cfg.TickRate = 60
			case "snapshot rate":
				cfg.SnapshotRate = 10
			case "grace period":
				cfg.ResumeGracePeriod /= 2
			case "invalid state":
				v.Entities[1].State = nil
			}
			before, cooling, waiting := c.Authoritative(), c.lastResync, c.resyncing
			if err := c.applyFull(encodeFull(v, cfg, session)); !errors.Is(err, ErrProtocol) {
				t.Error("invalid full state accepted", err)
			}
			if !reflect.DeepEqual(c.Authoritative(), before) || !bytes.Equal(c.predicted, []byte{3}) ||
				len(c.history) != 3 || len(c.recent) != 3 || c.sequence != 5 || c.lastAction != 42 ||
				c.cfg != DefaultConfig() || c.lastResync != cooling || c.resyncing != waiting || c.local != 1 {
				t.Error("rejected full state changed world, session or pending prediction")
			}
		})
	}
}

func TestFullStateRestoresResyncAndFreshConnection(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "resync", true: "fresh connection"}[fresh], func(t *testing.T) {
			c := pendingResyncClient(t)
			v := c.Authoritative()
			v.Tick++
			v.Entities[0].Ack = c.sequence
			v.Entities[0].State = []byte{9}
			if fresh {
				// Joining or resuming has no old local identity or input sequence.
				c = &Client{model: byteModel{}}
				v.Entities[0].ID = 3
				v.Entities[0].Generation = 7
				v.Entities[0].Ack = 99
			}
			started := time.Now()
			if err := c.applyFull(encodeFull(v, DefaultConfig(), sessionState{ResumeToken{1}, 43})); err != nil {
				t.Fatal("valid full state rejected", err)
			}
			if len(c.history) != 0 || len(c.recent) != 0 || c.resyncing || c.sequence != v.Entities[0].Ack ||
				c.lastAction != 43 || c.local != v.Entities[0].ID || !bytes.Equal(c.predicted, []byte{9}) {
				t.Fatal("full state did not restore authoritative progress")
			}
			if !fresh && c.lastResync.Before(started) {
				t.Fatal("resync response did not restart cooldown")
			}
		})
	}
}

func FuzzFullStateResync(f *testing.F) {
	cfg := DefaultConfig()
	entities := []Entity{{ID: 1, Generation: 1, Owner: 1, Dynamic: true, Ack: 5, State: []byte{9}}}
	v := View{Player: 1, Tick: 11, Revision: 1, Entities: entities}
	f.Add(encodeFull(v, cfg, sessionState{ResumeToken{1}, 43}))
	entities[0].Ack = 4
	f.Add(encodeFull(v, cfg, sessionState{ResumeToken{1}, 43}))
	f.Fuzz(func(t *testing.T, packet []byte) {
		if len(packet) > MaxFrame {
			return
		}
		c := pendingResyncClient(t)
		before, cooling := c.Authoritative(), c.lastResync
		if err := c.applyFull(packet); err != nil {
			if !reflect.DeepEqual(c.Authoritative(), before) || c.predicted[0] != 3 ||
				len(c.history) != 3 || len(c.recent) != 3 || c.sequence != 5 || c.lastAction != 42 ||
				c.cfg != cfg || c.lastResync != cooling || !c.resyncing || c.local != 1 || c.token != (ResumeToken{1}) {
				t.Fatal("rejected full state changed accepted state")
			}
		} else {
			local := c.entities[1]
			if c.local != 1 || local == nil || local.entity.Generation != 1 || local.entity.Owner != 1 || !local.entity.Dynamic ||
				local.entity.Ack != 5 || c.sequence != 5 || c.lastAction < 42 || c.cfg != cfg || c.token != (ResumeToken{1}) ||
				c.resyncing || len(c.history) != 0 || len(c.recent) != 0 || c.tick < 10 || c.revision < 1 {
				t.Fatal("full state violated session or resync invariants")
			}
		}
	})
}
