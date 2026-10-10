package loadgame

import (
	"bytes"
	syncnet "github.com/qihai-coding/go-statesync"
	"math"
	"testing"
)

func TestWorkload(t *testing.T) {
	for _, size := range []int{32, 512} {
		g, err := New(256, 16, size)
		if err != nil {
			t.Fatal(err)
		}
		for p := uint32(1); p <= 16; p++ {
			if _, err = g.Join(p); err != nil {
				t.Fatal(err)
			}
		}
		before := g.Entities()
		if len(before) != 256 {
			t.Fatal(len(before))
		}
		g.Step(1.0/30, []syncnet.PlayerInput{{Player: 1, Data: Move(1, 0)}})
		after := g.Entities()
		bots, owned := 0, 0
		for i, e := range after {
			if len(e.State) != size || !e.Dynamic || (Model{size}).ValidateState(e.State) != nil {
				t.Fatal("invalid entity", e.ID)
			}
			if e.Owner == 0 {
				bots++
				if bytes.Equal(before[i].State, e.State) {
					t.Fatal("stationary bot", e.ID)
				}
			} else {
				owned++
			}
		}
		if bots != 240 || owned != 16 || bytes.Equal(before[240].State, after[240].State) {
			t.Fatal("wrong population or player movement")
		}
		changes, _, err := g.Action(1, Stop())
		if err != nil || len(changes) != 0 {
			t.Fatal("stop failed", err)
		}
		frozen := g.Entities()
		g.Step(1.0/30, []syncnet.PlayerInput{{Player: 1, Data: Move(1, 0)}})
		for i, e := range g.Entities() {
			if !bytes.Equal(frozen[i].State, e.State) {
				t.Fatal("stop moved entity")
			}
			predicted, err := (Model{size}).Predict(e.State, Move(1, 0), 1.0/30)
			if err != nil || !bytes.Equal(predicted, e.State) {
				t.Fatal("stop prediction moved")
			}
		}
	}
	if (Model{}).ValidateInput(Move(float32(math.NaN()), 0)) == nil {
		t.Fatal("accepted NaN")
	}
	if (Model{}).ValidateState(make([]byte, 31)) == nil {
		t.Fatal("accepted truncated state")
	}
	if _, err := New(257, 16, 32); err == nil {
		t.Fatal("accepted too many entities")
	}
}
