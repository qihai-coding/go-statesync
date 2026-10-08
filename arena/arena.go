// Package arena is the explicitly encoded 2D example game.
package arena

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"

	syncnet "github.com/qihai-coding/go-statesync"
)

const (
	Player byte = 1
	Item   byte = 2
)
const Speed float32 = 6
const Limit float32 = 10

type State struct {
	Kind    byte
	X, Y    float32
	Pickups uint32
}

func Encode(s State) []byte {
	b := make([]byte, 13)
	b[0] = s.Kind
	binary.LittleEndian.PutUint32(b[1:5], math.Float32bits(s.X))
	binary.LittleEndian.PutUint32(b[5:9], math.Float32bits(s.Y))
	binary.LittleEndian.PutUint32(b[9:13], s.Pickups)
	return b
}
func Decode(b []byte) (State, error) {
	if len(b) != 13 {
		return State{}, syncnet.ErrProtocol
	}
	s := State{Kind: b[0], X: math.Float32frombits(binary.LittleEndian.Uint32(b[1:5])), Y: math.Float32frombits(binary.LittleEndian.Uint32(b[5:9])), Pickups: binary.LittleEndian.Uint32(b[9:13])}
	if s.Kind != Player && s.Kind != Item || !finite(s.X) || !finite(s.Y) || s.X < -Limit || s.X > Limit || s.Y < -Limit || s.Y > Limit {
		return s, syncnet.ErrProtocol
	}
	return s, nil
}
func finite(v float32) bool { return !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0) }
func Move(x, y float32) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b, math.Float32bits(x))
	binary.LittleEndian.PutUint32(b[4:], math.Float32bits(y))
	return b
}
func axes(b []byte) (float32, float32, error) {
	if len(b) != 8 {
		return 0, 0, syncnet.ErrProtocol
	}
	x := math.Float32frombits(binary.LittleEndian.Uint32(b))
	y := math.Float32frombits(binary.LittleEndian.Uint32(b[4:]))
	if !finite(x) || !finite(y) || x < -1 || x > 1 || y < -1 || y > 1 {
		return 0, 0, syncnet.ErrProtocol
	}
	return x, y, nil
}

type Model struct{}

func (Model) ValidateInput(b []byte) error { _, _, err := axes(b); return err }
func (Model) ValidateState(b []byte) error { _, err := Decode(b); return err }
func advance(s State, input []byte, dt float32) (State, error) {
	x, y, err := axes(input)
	if err != nil {
		return s, err
	}
	mag := x*x + y*y
	if mag > 1 {
		scale := float32(1 / math.Sqrt(float64(mag)))
		x *= scale
		y *= scale
	}
	s.X = max(-Limit, min(Limit, s.X+x*Speed*dt))
	s.Y = max(-Limit, min(Limit, s.Y+y*Speed*dt))
	return s, nil
}
func (Model) Predict(state, input []byte, dt float32) ([]byte, error) {
	s, err := Decode(state)
	if err != nil {
		return nil, err
	}
	s, err = advance(s, input, dt)
	return Encode(s), err
}
func (Model) Interpolate(a, b []byte, t float32) []byte {
	x, _ := Decode(a)
	y, _ := Decode(b)
	t = max(float32(0), min(float32(1), t))
	x.X += (y.X - x.X) * t
	x.Y += (y.Y - x.Y) * t
	return Encode(x)
}

type Game struct {
	entities map[uint32]syncnet.Entity
	players  map[uint32]uint32
	next     uint32
}

func New() *Game {
	g := &Game{entities: make(map[uint32]syncnet.Entity), players: make(map[uint32]uint32), next: 17}
	for i := uint32(1); i <= 16; i++ {
		// Item 1 starts at the origin so contention can be tested without navigation.
		x := float32((i-1)%4) * 2
		y := float32((i-1)/4) * 2
		g.entities[i] = syncnet.Entity{ID: i, Generation: 1, State: Encode(State{Kind: Item, X: x, Y: y})}
	}
	return g
}
func (g *Game) Join(player uint32) ([]syncnet.Change, error) {
	if _, ok := g.players[player]; ok {
		return nil, errors.New("already joined")
	}
	if g.next == 0 {
		return nil, errors.New("entity IDs exhausted")
	}
	e := syncnet.Entity{ID: g.next, Generation: 1, Owner: player, Dynamic: true, State: Encode(State{Kind: Player})}
	g.next++
	g.entities[e.ID] = e
	g.players[player] = e.ID
	return []syncnet.Change{{Entity: e}}, nil
}
func (g *Game) Leave(player uint32) []syncnet.Change {
	id, ok := g.players[player]
	if !ok {
		return nil
	}
	e := g.entities[id]
	delete(g.entities, id)
	delete(g.players, player)
	return []syncnet.Change{{Delete: true, Entity: e}}
}
func (g *Game) ValidateInput(b []byte) error { return (Model{}).ValidateInput(b) }
func (g *Game) Step(dt float32, inputs []syncnet.PlayerInput) []syncnet.Change {
	for _, in := range inputs {
		id, ok := g.players[in.Player]
		if !ok {
			continue
		}
		e := g.entities[id]
		s, _ := Decode(e.State)
		s, err := advance(s, in.Data, dt)
		if err == nil {
			e.State = Encode(s)
			g.entities[id] = e
		}
	}
	return nil
}
func Pickup(id, generation uint32) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b, id)
	binary.LittleEndian.PutUint32(b[4:], generation)
	return b
}
func (g *Game) Action(player uint32, b []byte) ([]syncnet.Change, []byte, error) {
	if len(b) != 8 {
		return nil, nil, syncnet.ErrProtocol
	}
	id, gen := binary.LittleEndian.Uint32(b), binary.LittleEndian.Uint32(b[4:])
	item, ok := g.entities[id]
	pid, pok := g.players[player]
	if !ok || !pok || item.Generation != gen {
		return nil, nil, errors.New("item unavailable")
	}
	is, _ := Decode(item.State)
	p := g.entities[pid]
	ps, _ := Decode(p.State)
	dx, dy := ps.X-is.X, ps.Y-is.Y
	if is.Kind != Item || dx*dx+dy*dy > 1 {
		return nil, nil, errors.New("out of range")
	}
	delete(g.entities, id)
	ps.Pickups++
	p.State = Encode(ps)
	g.entities[pid] = p
	return []syncnet.Change{{Delete: true, Entity: item}, {Entity: p}}, []byte{1}, nil
}
func (g *Game) Entities() []syncnet.Entity {
	es := make([]syncnet.Entity, 0, len(g.entities))
	for _, e := range g.entities {
		es = append(es, e)
	}
	sort.Slice(es, func(i, j int) bool { return es[i].ID < es[j].ID })
	return es
}
