// Package loadgame provides a deterministic, fixed-size dynamic workload.
package loadgame

import (
	"encoding/binary"
	"errors"
	"math"

	syncnet "github.com/qihai-coding/go-statesync"
)

const DefaultStateBytes = 32

// State stores float32 X/Y, a stopped flag, and stable padding.
type Model struct{ StateBytes int }

func (m Model) size() int {
	if m.StateBytes == 0 {
		return DefaultStateBytes
	}
	return m.StateBytes
}
func finite(v float32) bool { return !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0) }
func Move(x, y float32) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b, math.Float32bits(x))
	binary.LittleEndian.PutUint32(b[4:], math.Float32bits(y))
	return b
}
func axes(b []byte) (float32, float32) {
	return math.Float32frombits(binary.LittleEndian.Uint32(b)), math.Float32frombits(binary.LittleEndian.Uint32(b[4:]))
}
func (m Model) ValidateInput(b []byte) error {
	if len(b) != 8 {
		return syncnet.ErrProtocol
	}
	x, y := axes(b)
	if !finite(x) || !finite(y) || x < -1 || x > 1 || y < -1 || y > 1 {
		return syncnet.ErrProtocol
	}
	return nil
}
func (m Model) ValidateState(b []byte) error {
	if m.size() < DefaultStateBytes || m.size() > syncnet.MaxState || len(b) != m.size() {
		return syncnet.ErrProtocol
	}
	x, y := axes(b)
	if !finite(x) || !finite(y) || b[8] > 1 {
		return syncnet.ErrProtocol
	}
	return nil
}
func (m Model) Predict(state, input []byte, dt float32) ([]byte, error) {
	if err := m.ValidateState(state); err != nil {
		return nil, err
	}
	if err := m.ValidateInput(input); err != nil {
		return nil, err
	}
	if !finite(dt) || dt < 0 || dt > 1 {
		return nil, syncnet.ErrProtocol
	}
	b := append([]byte(nil), state...)
	if b[8] != 0 {
		return b, nil
	}
	x, y := axes(state)
	dx, dy := axes(input)
	x += dx * dt * 6
	y += dy * dt * 6
	binary.LittleEndian.PutUint32(b, math.Float32bits(x))
	binary.LittleEndian.PutUint32(b[4:], math.Float32bits(y))
	return b, nil
}
func (m Model) Interpolate(a, b []byte, fraction float32) []byte {
	out := append([]byte(nil), b...)
	x, y := axes(a)
	u, v := axes(b)
	t := max(float32(0), min(float32(1), fraction))
	binary.LittleEndian.PutUint32(out, math.Float32bits(x+(u-x)*t))
	binary.LittleEndian.PutUint32(out[4:], math.Float32bits(y+(v-y)*t))
	return out
}

type Game struct {
	Entropy    bool
	model      Model
	entities   []syncnet.Entity
	players    map[uint32]int
	maxPlayers int
	next       uint32
	tick       uint64
	stopped    bool
}

func New(entities, players, stateBytes int) (*Game, error) {
	if players < 1 || players > 16 || entities < players || entities > syncnet.MaxEntities || stateBytes < DefaultStateBytes || stateBytes > syncnet.MaxState {
		return nil, errors.New("invalid workload dimensions")
	}
	g := &Game{model: Model{StateBytes: stateBytes}, players: make(map[uint32]int), maxPlayers: players, next: uint32(entities - players + 1)}
	for i := 0; i < entities-players; i++ {
		g.entities = append(g.entities, g.entity(uint32(i+1), 0))
	}
	return g, nil
}
func (g *Game) entity(id, owner uint32) syncnet.Entity {
	b := make([]byte, g.model.size())
	for i := 9; i < len(b); i++ {
		b[i] = byte(int(id) + i)
	}
	return syncnet.Entity{ID: id, Generation: 1, Owner: owner, Dynamic: true, State: b}
}
func (g *Game) Join(player uint32) ([]syncnet.Change, error) {
	if _, ok := g.players[player]; ok || len(g.players) >= g.maxPlayers {
		return nil, errors.New("invalid workload join")
	}
	e := g.entity(g.next, player)
	g.next++
	g.players[player] = len(g.entities)
	g.entities = append(g.entities, e)
	return []syncnet.Change{{Entity: e}}, nil
}
func (g *Game) Leave(player uint32) []syncnet.Change {
	i, ok := g.players[player]
	if !ok {
		return nil
	}
	e := g.entities[i]
	delete(g.players, player)
	g.entities = append(g.entities[:i], g.entities[i+1:]...)
	for p, index := range g.players {
		if index > i {
			g.players[p] = index - 1
		}
	}
	return []syncnet.Change{{Delete: true, Entity: e}}
}
func (g *Game) ValidateInput(b []byte) error { return g.model.ValidateInput(b) }
func (g *Game) Step(dt float32, inputs []syncnet.PlayerInput) []syncnet.Change {
	if g.stopped {
		return nil
	}
	g.tick++
	moves := [4][]byte{Move(1, 0), Move(0, 1), Move(-1, 0), Move(0, -1)}
	for i, e := range g.entities {
		if e.Owner == 0 {
			g.entities[i].State, _ = g.model.Predict(e.State, moves[(g.tick/60+uint64(i))%4], dt)
		}
	}
	for _, in := range inputs {
		if i, ok := g.players[in.Player]; ok {
			if state, err := g.model.Predict(g.entities[i].State, in.Data, dt); err == nil {
				g.entities[i].State = state
			}
		}
	}
	if g.Entropy {
		for i, e := range g.entities {
			b := append([]byte(nil), e.State...)
			x := uint64(e.ID)*0x9e3779b97f4a7c15 + g.tick
			for j := 9; j < len(b); j++ {
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				b[j] = byte(x)
			}
			g.entities[i].State = b
		}
	}
	return nil
}

// Stop freezes every entity, including player prediction, for convergence checks.
func Stop() []byte { return []byte("stop") }
func (g *Game) Action(player uint32, b []byte) ([]syncnet.Change, []byte, error) {
	if _, ok := g.players[player]; !ok || string(b) != "stop" {
		return nil, nil, syncnet.ErrProtocol
	}
	g.stopped = true

	for i, e := range g.entities {
		e.State = append([]byte(nil), e.State...)
		e.State[8] = 1
		g.entities[i] = e

	}
	return nil, []byte("stopped"), nil
}
func (g *Game) Entities() []syncnet.Entity { return append([]syncnet.Entity(nil), g.entities...) }
