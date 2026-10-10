package statesync

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qihai-coding/go-statesync/internal/measure"
)

type inputBatch struct {
	peer   *peer
	inputs []Input
}
type player struct {
	peer                       *peer
	token                      ResumeToken
	expires                    time.Time
	ack                        uint64
	pending                    [inputWindow]Input
	lastAction                 uint64
	lastActionData, lastResult []byte
	lastResync                 time.Time
	controlled, generation     uint32
}
type Room struct {
	cfg                                                     Config
	game                                                    Game
	ctx                                                     context.Context
	cancel                                                  context.CancelCauseFunc
	done                                                    chan struct{}
	ingress                                                 sync.RWMutex
	commands                                                chan roomCommand
	inputs                                                  chan inputBatch
	players                                                 map[uint32]*player
	entities                                                map[uint32]Entity
	tick, revision                                          uint64
	snapshotAccumulator                                     int
	histogram                                               [1001]uint64
	totalStep, maxStep                                      time.Duration
	started                                                 time.Duration
	inputDrops, snapshotDrops, reliableBytes, datagramBytes atomic.Uint64
	deltaBytes, fullRecordBytes, deltaRecords, fullRecords  atomic.Uint64
	expectedEntityUpdates, sentEntityUpdates                atomic.Uint64
	cleanupErr                                              atomic.Pointer[error]
}

// Construction only allocates room-owned state; initialization runs in start.
func newRoom(parent context.Context, cfg Config, game Game) *Room {
	ctx, cancel := context.WithCancelCause(parent)
	return &Room{cfg: cfg, game: game, ctx: ctx, cancel: cancel, done: make(chan struct{}), started: measure.Now(),
		commands: make(chan roomCommand, 128), inputs: make(chan inputBatch, 256),
		players: make(map[uint32]*player), entities: make(map[uint32]Entity)}
}

// start is called once after reserving the name, outside the server registry lock.
func (r *Room) start() (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("game initialization panic: %v", v)
		}
		if err != nil {
			r.cancel(err)
			r.finish()
		}
	}()
	if r.ctx.Err() != nil {
		return ErrClosed
	}
	initial := r.game.Entities()
	if len(initial) > MaxEntities {
		return errors.New("too many initial entities")
	}
	for _, e := range initial {
		if !validEntity(e) {
			return errors.New("invalid initial entity")
		}
		if _, ok := r.entities[e.ID]; ok {
			return errors.New("duplicate initial entity")
		}
		r.entities[e.ID] = e
	}
	if r.ctx.Err() != nil {
		return ErrClosed
	}
	go r.run()
	return nil
}
func (r *Room) Close()                { r.cancel(nil); <-r.done }
func (r *Room) Done() <-chan struct{} { return r.done }

// Called once after initialization or the owner loop ends. Admission must have
// stopped before dropping channels: a retained Room must not retain their queues.
func (r *Room) finish() {
	r.ingress.Lock()
	defer r.ingress.Unlock()
	r.game = nil
	r.entities = nil
	r.players = nil
	r.commands = nil
	r.inputs = nil
	close(r.done)
}

func (r *Room) Err() error {
	err := context.Cause(r.ctx)
	if errors.Is(err, context.Canceled) {
		if cleanup := r.cleanupErr.Load(); cleanup != nil {
			return *cleanup
		}
		return nil
	}
	return err
}
func (r *Room) command(c roomCommand) bool {
	r.ingress.RLock()
	if r.ctx.Err() != nil {
		r.ingress.RUnlock()
		return false
	}
	select {
	case r.commands <- c:
		r.ingress.RUnlock()
		return true
	default:
		r.ingress.RUnlock()
		if c.peer != nil {
			c.peer.kick(closeBusy, "room control queue full")
		}
		return false
	}
}

func (r *Room) enqueueInputs(batch inputBatch) bool {
	r.ingress.RLock()
	defer r.ingress.RUnlock()
	if r.ctx.Err() != nil {
		return false
	}
	select {
	case r.inputs <- batch:
	default:
		r.inputDrops.Add(1)
	}
	return true
}

func (r *Room) Inspect(ctx context.Context) (View, Metrics, error) {
	r.ingress.RLock()
	if r.ctx.Err() != nil {
		r.ingress.RUnlock()
		return View{}, Metrics{}, ErrClosed
	}
	reply := make(chan inspectResult, 1)
	select {
	case r.commands <- roomCommand{inspect: reply}:
	case <-ctx.Done():
		r.ingress.RUnlock()
		return View{}, Metrics{}, ctx.Err()
	case <-r.ctx.Done():
		r.ingress.RUnlock()
		return View{}, Metrics{}, ErrClosed
	}
	r.ingress.RUnlock()
	select {
	case out := <-reply:
		return out.view, out.metrics, nil
	case <-ctx.Done():
		return View{}, Metrics{}, ctx.Err()
	case <-r.done:
		return View{}, Metrics{}, ErrClosed
	}
}
func (r *Room) run() {
	defer func() {
		if v := recover(); v != nil {
			r.cancel(fmt.Errorf("room panic: %v", v))
		}
		for id, p := range r.players {
			delete(r.players, id)
			if p.peer != nil {
				p.peer.kick(closeRoom, "room closed")
			}
			func() {
				defer func() {
					if v := recover(); v != nil {
						err := fmt.Errorf("room leave cleanup panic: %v", v)
						r.cleanupErr.CompareAndSwap(nil, &err)
					}
				}()
				// No broadcasts after termination; release each session exactly once.
				r.game.Leave(id)
			}()
		}
		r.finish()
	}()
	interval := time.Second / time.Duration(r.cfg.TickRate)
	next := time.Now().Add(interval)
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		if r.ctx.Err() != nil {
			return
		}
		select {
		case <-r.ctx.Done():
			return
		case c := <-r.commands:
			if err := r.handle(c); err != nil {
				r.cancel(err)
				return
			}
		case b := <-r.inputs:
			r.receive(b)
		case <-timer.C:
			steps := 0
			for !time.Now().Before(next) && steps < 4 {
				started := measure.Now()
				if err := r.step(); err != nil {
					r.cancel(err)
					return
				}
				elapsed := measure.Now() - started
				r.totalStep += elapsed
				r.maxStep = max(r.maxStep, elapsed)
				r.histogram[min(int(elapsed/(100*time.Microsecond)), 1000)]++
				next = next.Add(interval)
				steps++
			}
			if time.Since(next) > time.Second {
				r.cancel(errors.New("room more than one second behind"))
				return
			}
			timer.Reset(max(time.Until(next), time.Nanosecond))
		}
	}
}

// Session transitions run only in the room loop.
func (r *Room) sweep(now time.Time) error {
	for id, p := range r.players {
		if p.peer != nil && !p.peer.active() {
			if r.cfg.ResumeGracePeriod == 0 || !p.peer.resumable() {
				delete(r.players, id)
				if err := r.apply(r.game.Leave(id), nil); err != nil {
					return err
				}
				continue
			}
			p.peer = nil
			p.pending = [inputWindow]Input{}
			p.expires = now.Add(r.cfg.ResumeGracePeriod)
		}
		if p.peer == nil && !now.Before(p.expires) {
			delete(r.players, id)
			if err := r.apply(r.game.Leave(id), nil); err != nil {
				return err
			}
		}
	}
	return nil
}
func (r *Room) receive(batch inputBatch) {
	p := r.players[batch.peer.id]
	if p == nil || p.peer != batch.peer || !batch.peer.active() {
		return
	}
	for _, in := range batch.inputs {
		if r.game.ValidateInput(in.Data) != nil {
			p.peer.kick(closeProtocol, "invalid input")
			return
		}
		if in.Sequence <= p.ack {
			continue
		}
		if in.Sequence-p.ack > inputWindow {
			p.peer.kick(closeProtocol, "input sequence outside window")
			return
		}
		slot := in.Sequence % inputWindow
		if p.pending[slot].Sequence != in.Sequence {
			p.pending[slot] = in
		}
	}
}
func (r *Room) step() error {
	r.tick++
	if err := r.sweep(time.Now()); err != nil {
		return err
	}
	for n := len(r.inputs); n > 0; n-- {
		r.receive(<-r.inputs)
	}
	var chosen [16]PlayerInput
	n := 0
	for id, p := range r.players {
		if p.peer == nil || !p.peer.active() {
			continue
		}
		seq := uint64(0)
		index := 0
		// ponytail: 256 slots x 16 players; index the ring if profiling warrants it.
		for i, v := range p.pending {
			if v.Sequence > p.ack && (seq == 0 || v.Sequence < seq) {
				seq = v.Sequence
				index = i
			}
		}
		if seq != 0 {
			chosen[n] = PlayerInput{Player: id, Data: p.pending[index].Data}
			n++
			p.pending[index] = Input{}
			p.ack = seq
		}
	}
	sort.Slice(chosen[:n], func(i, j int) bool { return chosen[i].Player < chosen[j].Player })
	if err := r.apply(r.game.Step(1/float32(r.cfg.TickRate), chosen[:n]), nil); err != nil {
		return err
	}
	r.snapshotAccumulator += r.cfg.SnapshotRate
	if r.snapshotAccumulator >= r.cfg.TickRate {
		r.snapshotAccumulator -= r.cfg.TickRate
		entities, err := r.capture()
		if err != nil {
			return err
		}
		groups, err := snapshotGroups(entities, r.cfg.DatagramSize)
		if err != nil {
			return err
		}
		view := View{Tick: r.tick, Revision: r.revision, ServerTime: measure.Now() - r.started}
		for _, p := range r.players {
			if p.peer != nil && p.peer.active() {
				packets, stats, err := p.peer.rep.build(view, groups, r.cfg.SnapshotEncoding)
				if err != nil {
					return err
				}
				r.deltaBytes.Add(stats.deltaBytes)
				r.fullRecordBytes.Add(stats.fullBytes)
				r.deltaRecords.Add(stats.deltaRecords)
				r.fullRecords.Add(stats.fullRecords)
				r.expectedEntityUpdates.Add(stats.deltaRecords + stats.fullRecords)
				p.peer.offer(packets)
			}
		}
	}
	return nil
}
func (r *Room) controlledValid(entities map[uint32]Entity) error {
	counts := make(map[uint32]int, len(r.players))
	for _, e := range entities {
		if p := r.players[e.Owner]; p != nil {
			counts[e.Owner]++
			if !e.Dynamic || p.controlled != 0 && (e.ID != p.controlled || e.Generation != p.generation) {
				return errors.New("controlled entity must remain unchanged for the session")
			}
		}
	}
	for id := range r.players {
		if counts[id] != 1 {
			return errors.New("session requires exactly one controlled entity")
		}
	}
	return nil
}
func (r *Room) capture() ([]Entity, error) {
	all := r.game.Entities()
	if len(all) != len(r.entities) || len(all) > MaxEntities {
		return nil, errors.New("game lifecycle and entity list disagree")
	}
	seen := make(map[uint32]bool, len(all))
	for i, e := range all {
		known, ok := r.entities[e.ID]
		if !ok || !validEntity(e) || seen[e.ID] || e.Generation != known.Generation || e.Owner != known.Owner || e.Dynamic != known.Dynamic {
			return nil, errors.New("invalid game snapshot")
		}
		seen[e.ID] = true
		if p := r.players[e.Owner]; p != nil {
			all[i].Ack = p.ack
		}
	}
	return all, nil
}
func (r *Room) apply(changes []Change, exclude *peer) error {
	if len(changes) == 0 {
		return nil
	}
	if len(changes) > MaxEntities*2 {
		return errors.New("too many changes")
	}
	next := maps.Clone(r.entities)
	generations := make(map[uint32]uint32, len(changes))
	wire := make([]Change, len(changes))
	for i, c := range changes {
		e := c.Entity
		if !validEntity(e) {
			return errors.New("invalid entity change")
		}
		old, exists := next[e.ID]
		floor := generations[e.ID]
		if c.Delete {
			if !exists || old.Generation != e.Generation {
				return errors.New("invalid deletion")
			}
			generations[e.ID] = max(floor, old.Generation)
			delete(next, e.ID)
		} else {
			if e.Generation <= floor || exists && (e.Generation < old.Generation || e.Generation == old.Generation && (e.Owner != old.Owner || e.Dynamic != old.Dynamic)) {
				return errors.New("invalid entity generation")
			}
			if p := r.players[e.Owner]; p != nil {
				e.Ack = p.ack
			}
			next[e.ID] = e
		}
		wire[i] = Change{Delete: c.Delete, Entity: e}
	}
	if len(next) > MaxEntities {
		return errors.New("entity limit exceeded")
	}
	if err := r.controlledValid(next); err != nil {
		return err
	}
	if r.revision == ^uint64(0) {
		return errors.New("revision exhausted")
	}
	b := encodeChanges(r.tick, r.revision+1, measure.Now()-r.started, wire)
	if len(b) > MaxFrame {
		return errors.New("change batch exceeds frame limit")
	}
	// Commit only after validation; game state itself is not rolled back on failure.
	r.entities = next
	r.revision++
	for _, p := range r.players {
		if p.peer != nil && p.peer != exclude && p.peer.active() {
			if p.peer.send(b) {
				if err := p.peer.rep.installChanges(wire, r.revision); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
func (r *Room) full(p *player) error {
	es, err := r.capture()
	if err != nil {
		return err
	}
	v := View{Tick: r.tick, Revision: r.revision, Player: p.peer.id, Entities: es, ServerTime: measure.Now() - r.started}
	epoch, err := p.peer.rep.installFull(v)
	if err != nil {
		return err
	}
	p.peer.send(encodeFull(v, r.cfg, sessionState{token: p.token, lastAction: p.lastAction, epoch: epoch}))
	return nil
}
func actionResult(id uint64, ok bool, data []byte) []byte {
	b := start(msgResult)
	b.u64(id)
	if ok {
		b.u8(1)
	} else {
		b.u8(0)
	}
	b.bytes(data)
	return b
}
func (r *Room) handle(c roomCommand) error {
	if err := r.sweep(time.Now()); err != nil {
		return err
	}
	if c.inspect != nil {
		es, err := r.capture()
		if err != nil {
			return err
		}
		for i := range es {
			es[i] = cloneEntity(es[i])
		}
		m := Metrics{Ticks: r.tick, StepTotal: r.totalStep, StepMax: r.maxStep, InputQueue: len(r.inputs), ControlQueue: len(r.commands),
			InputDrops: r.inputDrops.Load(), SnapshotDrops: r.snapshotDrops.Load(), ReliableBytes: r.reliableBytes.Load(), DatagramBytes: r.datagramBytes.Load(),
			DeltaBytes: r.deltaBytes.Load(), FullRecordBytes: r.fullRecordBytes.Load(), DeltaRecords: r.deltaRecords.Load(), FullRecords: r.fullRecords.Load(),
			ExpectedEntityUpdates: r.expectedEntityUpdates.Load(), SentEntityUpdates: r.sentEntityUpdates.Load()}
		m.SnapshotRecords = m.DeltaRecords + m.FullRecords
		target := (r.tick*99 + 99) / 100
		sum := uint64(0)
		for i, v := range r.histogram {
			sum += v
			if r.tick > 0 && sum >= target {
				m.StepP99 = time.Duration(i+1) * 100 * time.Microsecond
				break
			}
		}
		for _, p := range r.players {
			if p.peer == nil {
				m.RetainedPlayers++
				continue
			}
			m.Players++
			m.ReliableQueued += len(p.peer.reliable)
			m.SnapshotQueued += len(p.peer.snapshots)
			m.BaselineEntities += len(p.peer.rep.bases)
			m.BaselineBytes += p.peer.rep.baselineBytes()
		}
		c.inspect <- inspectResult{View{Tick: r.tick, Revision: r.revision, Entities: es, ServerTime: measure.Now() - r.started}, m}
		return nil
	}
	if !c.peer.active() {
		if c.reply != nil {
			c.reply <- ErrClosed
		}
		return nil
	}
	if c.kind == msgResume {
		if r.cfg.ResumeGracePeriod > 0 && c.token != (ResumeToken{}) {
			for id, p := range r.players {
				if subtle.ConstantTimeCompare(p.token[:], c.token[:]) != 1 {
					continue
				}
				old := p.peer
				c.peer.id = id
				p.peer = c.peer
				p.pending = [inputWindow]Input{}
				p.expires = time.Time{}
				if old != nil {
					old.kick(closeReplaced, "session replaced")
				}
				err := r.full(p)
				c.reply <- err
				return err
			}
		}
		c.reply <- ErrResumeRejected
		return nil
	}
	if c.kind == msgJoin {
		if len(r.players) >= r.cfg.MaxPlayers {
			c.reply <- errors.New("room full")
			return nil
		}
		p := &player{peer: c.peer}
		if r.cfg.ResumeGracePeriod > 0 {
			if _, err := rand.Read(p.token[:]); err != nil {
				c.reply <- err
				return nil
			}
		}
		changes, err := r.game.Join(c.peer.id)
		if err != nil {
			c.reply <- err
			return nil
		}
		r.players[c.peer.id] = p
		if err = r.apply(changes, c.peer); err != nil {
			c.reply <- err
			return err
		}
		if err = r.controlledValid(r.entities); err != nil {
			c.reply <- err
			return err
		}
		for _, e := range r.entities {
			if e.Owner == c.peer.id {
				p.controlled = e.ID
				p.generation = e.Generation
			}
		}
		err = r.full(p)
		c.reply <- err
		return err
	}
	p := r.players[c.peer.id]
	if p == nil || p.peer != c.peer {
		return nil
	}
	switch c.kind {
	case msgBaselineAck:
		epoch, revision, available, err := decodeBaselineAck(c.data)
		if err != nil || p.peer.rep.acknowledge(epoch, revision, available) != nil {
			p.peer.kick(closeProtocol, "invalid baseline acknowledgement")
		}
	case msgAction:
		d := decode(c.data, msgAction)
		op := d.u64()
		payload := d.bytes(MaxAction)
		if d.end() != nil || op == 0 {
			p.peer.kick(closeProtocol, "invalid action")
			return nil
		}
		if op == p.lastAction {
			if !bytes.Equal(payload, p.lastActionData) {
				p.peer.send(actionResult(op, false, []byte("action ID payload mismatch")))
			} else {
				p.peer.send(p.lastResult)
			}
			return nil
		}
		if op < p.lastAction {
			p.peer.send(actionResult(op, false, []byte("stale action ID")))
			return nil
		}
		changes, result, err := r.game.Action(c.peer.id, payload)
		if err != nil {
			result = []byte("action rejected")
		} else {
			if len(result) > MaxAction {
				return errors.New("action result exceeds limit")
			}
			if err := r.apply(changes, nil); err != nil {
				return err
			}
		}
		p.lastAction = op
		p.lastActionData = append(p.lastActionData[:0], payload...)
		p.lastResult = actionResult(op, err == nil, result)
		p.peer.send(p.lastResult)
	case msgResync:
		d := decode(c.data, msgResync)
		cutoff := d.u64()
		if d.end() != nil || cutoff < p.ack || cutoff-p.ack > 4096 {
			p.peer.kick(closeProtocol, "invalid resync")
			return nil
		}
		if !p.lastResync.IsZero() && time.Since(p.lastResync) < time.Second {
			p.peer.kick(closeProtocol, "resync rate exceeded")
			return nil
		}
		p.lastResync = time.Now()
		p.ack = cutoff
		p.pending = [inputWindow]Input{}
		return r.full(p)
	default:
		return errors.New("unknown room command")
	}
	return nil
}
