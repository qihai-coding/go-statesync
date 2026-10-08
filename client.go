package statesync

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
)

type sample struct {
	tick  uint64
	state []byte
}
type tracked struct {
	entity  Entity
	tick    uint64
	samples []sample
}
type actionReply struct {
	id   uint64
	data []byte
	err  error
}

type Client struct {
	conn                     *quic.Conn
	stream                   *quic.Stream
	model                    Model
	mu                       sync.Mutex
	actionGate               chan struct{}
	lastSubmitted            uint64
	lastSubmittedData        []byte
	token                    ResumeToken
	lastAction               uint64
	entities                 map[uint32]*tracked
	player, local            uint32
	cfg                      Config
	revision, tick, sequence uint64
	anchor                   uint64
	anchorAt                 time.Time
	history                  []Input
	recent                   []Input
	predicted                []byte
	resyncing                bool
	lastResync               time.Time
	buffer                   time.Duration
	reliable                 chan []byte
	outgoing                 chan []byte
	replies                  chan actionReply
	pendingAction            uint64
	wg                       sync.WaitGroup
	closeOnce                sync.Once
	lastError                error
	received                 atomic.Uint64
	sent                     atomic.Uint64
	resyncs                  atomic.Uint64
}

func Dial(ctx context.Context, addr, room string, tlsConfig *tls.Config, model Model) (*Client, error) {
	return dial(ctx, addr, room, tlsConfig, model, ResumeToken{}, false)
}

func DialResume(ctx context.Context, addr, room string, tlsConfig *tls.Config, model Model, token ResumeToken) (*Client, error) {
	if token == (ResumeToken{}) {
		return nil, ErrResumeRejected
	}
	return dial(ctx, addr, room, tlsConfig, model, token, true)
}

func dial(ctx context.Context, addr, room string, tlsConfig *tls.Config, model Model, token ResumeToken, resume bool) (*Client, error) {
	if tlsConfig == nil || tlsConfig.InsecureSkipVerify || model == nil || !validRoomName(room) {
		return nil, errors.New("verified TLS config, room and model required")
	}
	tc := tlsConfig.Clone()
	tc.MinVersion = tls.VersionTLS13
	tc.NextProtos = []string{ALPN}
	conn, err := quic.DialAddr(ctx, addr, tc, transportConfig())
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Client, error) {
		if ctx.Err() != nil {
			err = context.Cause(ctx)
		} else {
			err = connectionError(conn, err)
		}
		conn.CloseWithError(closeTemporary, "join failed")
		return nil, err
	}
	if !conn.ConnectionState().SupportsDatagrams.Remote {
		return fail(errors.New("datagrams not negotiated"))
	}
	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return fail(err)
	}
	stop := context.AfterFunc(ctx, func() { conn.CloseWithError(closeTemporary, "join canceled") })
	defer stop()
	st.SetDeadline(time.Now().Add(6 * time.Second))
	b := start(msgJoin)
	if resume {
		b = start(msgResume)
	}
	b.bytes([]byte(room))
	b = append(b, token[:]...)
	if err = writeFrame(st, b); err != nil {
		return fail(err)
	}
	initial, err := readFrame(st, MaxFrame)
	if err != nil {
		return fail(err)
	}
	c := &Client{conn: conn, stream: st, model: model, entities: make(map[uint32]*tracked),
		buffer: 200 * time.Millisecond, reliable: make(chan []byte, 64), outgoing: make(chan []byte, 1), replies: make(chan actionReply, 1), actionGate: make(chan struct{}, 1)}
	if err = c.applyFull(initial); err != nil {
		return fail(err)
	}
	// Hand off the connection only if cancellation has not claimed it.
	if !stop() {
		return fail(context.Cause(ctx))
	}
	if resume {
		c.lastResync = time.Now()
	}
	st.SetDeadline(time.Time{})
	c.received.Add(uint64(len(initial) + 4))
	c.sent.Add(uint64(len(b) + 4))
	c.wg.Add(4)
	go c.readReliable()
	go c.readDatagrams()
	go c.writeReliable()
	go c.writeDatagrams()
	return c, nil
}
func (c *Client) Done() <-chan struct{} { return c.conn.Context().Done() }
func (c *Client) Close() {
	c.close(closeLeave)
}

// Disconnect closes transport but allows the session to be resumed.
func (c *Client) Disconnect() { c.close(closeTemporary) }
func (c *Client) close(code quic.ApplicationErrorCode) {
	c.closeOnce.Do(func() { c.conn.CloseWithError(code, "client closed") })
	c.wg.Wait()
}
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastError != nil {
		return connectionError(c.conn, c.lastError)
	}
	return connectionError(c.conn, c.conn.Context().Err())
}
func (c *Client) fail(err error) {
	err = connectionError(c.conn, err)
	c.mu.Lock()
	if c.lastError == nil {
		c.lastError = err
	}
	c.mu.Unlock()
	code := closeTemporary
	if errors.Is(err, ErrProtocol) {
		code = closeProtocol
	}
	c.conn.CloseWithError(code, "client connection ended")
}
func (c *Client) ResumeToken() ResumeToken { c.mu.Lock(); defer c.mu.Unlock(); return c.token }
func (c *Client) LastActionID() uint64     { c.mu.Lock(); defer c.mu.Unlock(); return c.lastAction }
func (c *Client) ResumeGracePeriod() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg.ResumeGracePeriod
}
func (c *Client) TickRate() int  { c.mu.Lock(); defer c.mu.Unlock(); return c.cfg.TickRate }
func (c *Client) Player() uint32 { c.mu.Lock(); defer c.mu.Unlock(); return c.player }
func (c *Client) SetInterpolationDelay(delay time.Duration) error {
	if delay < 0 || delay > 2*time.Second {
		return errors.New("interpolation delay outside 0..2s")
	}
	c.mu.Lock()
	c.buffer = delay
	c.mu.Unlock()
	return nil
}
func (c *Client) Bytes() (sent, received uint64) { return c.sent.Load(), c.received.Load() }
func (c *Client) ResyncCount() uint64            { return c.resyncs.Load() }
func (c *Client) queueReliable(b []byte) error {
	select {
	case <-c.Done():
		return ErrClosed
	default:
	}
	select {
	case c.reliable <- b:
		return nil
	default:
		return ErrBusy
	}
}

// SubmitInput predicts exactly one fixed simulation step. Call at TickRate.
// The server independently caps processing to one input per player per tick.
func (c *Client) SubmitInput(data []byte) error {
	if len(data) > MaxInput || c.model.ValidateInput(data) != nil {
		return ErrProtocol
	}
	c.mu.Lock()
	if c.conn.Context().Err() != nil {
		c.mu.Unlock()
		return ErrClosed
	}
	if c.resyncing {
		c.mu.Unlock()
		return nil
	}
	if len(c.history) >= HistorySize {
		err := c.resyncLocked()
		c.mu.Unlock()
		return err
	}
	predicted, err := c.model.Predict(c.predicted, data, 1/float32(c.cfg.TickRate))
	if err != nil {
		c.mu.Unlock()
		return err
	}
	c.sequence++
	if c.sequence == 0 {
		c.mu.Unlock()
		c.fail(ErrProtocol)
		return ErrProtocol
	}
	in := Input{c.sequence, append([]byte(nil), data...)}
	c.history = append(c.history, in)
	if len(c.recent) == 3 {
		copy(c.recent, c.recent[1:])
		c.recent = c.recent[:2]
	}
	c.recent = append(c.recent, in)
	c.predicted = predicted
	packet := encodeInputs(c.recent)
	c.mu.Unlock()
	select {
	case c.outgoing <- packet:
		return nil
	default:
	}
	select {
	case <-c.outgoing:
	default:
	}
	select {
	case c.outgoing <- packet:
	default:
	}
	return nil
}
func (c *Client) resyncLocked() error {
	if c.resyncing {
		return nil
	}
	if !c.lastResync.IsZero() && time.Since(c.lastResync) < time.Second {
		return ErrBusy
	}
	b := start(msgResync)
	b.u64(c.sequence)
	if err := c.queueReliable(b); err != nil {
		return err
	}
	c.resyncing = true
	c.lastResync = time.Now()
	c.resyncs.Add(1)
	return nil
}
func (c *Client) Resync() error { c.mu.Lock(); defer c.mu.Unlock(); return c.resyncLocked() }

// Action uses a caller-supplied monotonically increasing request ID.
// Retrying the same ID and payload in this session returns the previous result.
func (c *Client) Action(ctx context.Context, id uint64, data []byte) ([]byte, error) {
	if id == 0 || len(data) > MaxAction {
		return nil, ErrProtocol
	}
	select {
	case c.actionGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.Done():
		return nil, c.Err()
	}
	defer func() {
		c.mu.Lock()
		c.pendingAction = 0
		c.mu.Unlock()
		<-c.actionGate
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The gate owns these fields. Reject conflicting retries before an older
	// in-flight reply with the same ID can be mistaken for the new payload.
	if id < c.lastSubmitted || id == c.lastSubmitted && !bytes.Equal(data, c.lastSubmittedData) {
		return nil, errors.New("action ID must increase or retry the same payload")
	}
	c.mu.Lock()
	c.pendingAction = id
	select {
	case <-c.replies:
	default:
	}
	c.mu.Unlock()
	b := start(msgAction)
	b.u64(id)
	b.bytes(data)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.queueReliable(b); err != nil {
		return nil, err
	}
	c.lastSubmitted = id
	c.lastSubmittedData = append(c.lastSubmittedData[:0], data...)
	for {
		select {
		case r := <-c.replies:
			if r.id == id {
				return r.data, r.err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.Done():
			return nil, c.Err()
		}
	}
}
func (c *Client) Authoritative() View {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := View{Tick: c.tick, Revision: c.revision, Player: c.player, Entities: make([]Entity, 0, len(c.entities))}
	for _, t := range c.entities {
		v.Entities = append(v.Entities, cloneEntity(t.entity))
	}
	sort.Slice(v.Entities, func(i, j int) bool { return v.Entities[i].ID < v.Entities[j].ID })
	return v
}

// Sample returns predicted local state or a delayed interpolated remote state.
func (c *Client) Sample(id uint32, now time.Time) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.entities[id]
	if t == nil {
		return nil, false
	}
	if id == c.local {
		return append([]byte(nil), c.predicted...), true
	}
	if !t.entity.Dynamic || len(t.samples) == 0 {
		return append([]byte(nil), t.entity.State...), true
	}
	target := float64(c.anchor) + now.Sub(c.anchorAt).Seconds()*float64(c.cfg.TickRate) - c.buffer.Seconds()*float64(c.cfg.TickRate)
	samples := t.samples
	if target <= float64(samples[0].tick) {
		return append([]byte(nil), samples[0].state...), true
	}
	for i := 1; i < len(samples); i++ {
		if target <= float64(samples[i].tick) {
			fraction := float32((target - float64(samples[i-1].tick)) / float64(samples[i].tick-samples[i-1].tick))
			return c.model.Interpolate(samples[i-1].state, samples[i].state, fraction), true
		}
	}
	return append([]byte(nil), samples[len(samples)-1].state...), true
}
func (c *Client) anchorTime(tick uint64) {
	if tick > c.anchor || c.anchorAt.IsZero() {
		c.anchor = tick
		c.anchorAt = time.Now()
	}
	c.tick = max(c.tick, tick)
}
func (c *Client) applyFull(b []byte) error {
	v, cfg, session, err := decodeFull(b)
	if err != nil {
		return err
	}
	if c.player != 0 && (!c.resyncing || v.Player != c.player || v.Tick < c.tick || v.Revision < c.revision ||
		session.token != c.token || session.lastAction < c.lastAction || cfg != c.cfg) {
		return ErrProtocol
	}
	entities := make(map[uint32]*tracked, len(v.Entities))
	local := uint32(0)
	for _, e := range v.Entities {
		if c.model.ValidateState(e.State) != nil {
			return ErrProtocol
		}
		if e.Owner == v.Player {
			if local != 0 || !e.Dynamic {
				return ErrProtocol
			}
			local = e.ID
		}
		entities[e.ID] = &tracked{entity: e, tick: v.Tick, samples: []sample{{v.Tick, e.State}}}
	}
	if local == 0 {
		return ErrProtocol
	}
	if c.player != 0 {
		old := c.entities[c.local]
		if local != c.local || old == nil || entities[local].entity.Generation != old.entity.Generation || entities[local].entity.Ack != c.sequence {
			return ErrProtocol
		}
	}
	c.entities = entities
	c.player = v.Player
	c.local = local
	c.cfg = cfg
	c.token = session.token
	c.lastAction = session.lastAction
	c.revision = v.Revision
	c.sequence = entities[local].entity.Ack
	c.history = nil
	c.recent = nil
	c.predicted = append([]byte(nil), entities[local].entity.State...)
	if c.resyncing {
		c.lastResync = time.Now()
	}
	c.resyncing = false
	c.anchor = v.Tick
	c.anchorAt = time.Now()
	c.tick = v.Tick
	return nil
}
func (c *Client) reconcile(e Entity) error {
	if c.resyncing {
		return nil
	}
	if e.Ack > c.sequence {
		return ErrProtocol
	}
	if len(c.history) > 0 && e.Ack+1 < c.history[0].Sequence {
		err := c.resyncLocked()
		if errors.Is(err, ErrBusy) {
			return nil
		}
		return err
	}
	cut := sort.Search(len(c.history), func(i int) bool { return c.history[i].Sequence > e.Ack })
	// Clear discarded references so a small logical history cannot retain large payloads.
	copy(c.history, c.history[cut:])
	clear(c.history[len(c.history)-cut:])
	c.history = c.history[:len(c.history)-cut]
	c.predicted = append(c.predicted[:0], e.State...)
	for _, in := range c.history {
		var err error
		c.predicted, err = c.model.Predict(c.predicted, in.Data, 1/float32(c.cfg.TickRate))
		if err != nil {
			return err
		}
	}
	return nil
}
func (c *Client) update(e Entity, tick uint64, force bool) error {
	t := c.entities[e.ID]
	if t != nil && !force && (tick <= t.tick || e.Generation != t.entity.Generation) {
		return nil
	}
	if c.model.ValidateState(e.State) != nil {
		return ErrProtocol
	}
	if t == nil || t.entity.Generation != e.Generation {
		t = &tracked{}
		c.entities[e.ID] = t
	}
	t.entity = e
	t.tick = tick
	if n := len(t.samples); n > 0 && t.samples[n-1].tick == tick {
		t.samples[n-1] = sample{tick, e.State}
	} else {
		// Bounded history covers 2s at the maximum configured snapshot rate (120 Hz).
		if len(t.samples) >= 256 {
			copy(t.samples, t.samples[1:])
			t.samples = t.samples[:255]
		}
		t.samples = append(t.samples, sample{tick, e.State})
	}
	if e.ID == c.local {
		return c.reconcile(e)
	}
	return nil
}
func (c *Client) applyChanges(b []byte) error {
	tick, rev, changes, err := decodeChanges(b)
	if err != nil {
		return err
	}
	if c.revision == ^uint64(0) || rev != c.revision+1 || tick < c.tick {
		return ErrProtocol
	}
	// Match the server's final-count rule: a full room may create before deleting
	// within one batch. Stage only metadata for touched IDs, retaining a deleted
	// generation as the floor for any later recreation within this batch.
	type stagedEntity struct {
		generation, owner uint32
		ack               uint64
		dynamic, exists   bool
	}
	count := len(c.entities)
	staged := make(map[uint32]stagedEntity, len(changes))
	for _, ch := range changes {
		e := ch.Entity
		old, touched := staged[e.ID]
		if !touched {
			if t := c.entities[e.ID]; t != nil {
				v := t.entity
				old = stagedEntity{v.Generation, v.Owner, v.Ack, v.Dynamic, true}
			}
		}
		if ch.Delete {
			if !old.exists || old.generation != e.Generation || e.ID == c.local {
				return ErrProtocol
			}
			count--
			old.exists = false
		} else {
			if e.Generation < old.generation || e.Generation == old.generation && (!old.exists || e.Owner != old.owner || e.Dynamic != old.dynamic) {
				return ErrProtocol
			}
			if e.Owner == c.player && e.ID != c.local || e.ID == c.local &&
				(e.Owner != c.player || e.Generation != old.generation || !e.Dynamic || e.Ack < old.ack || e.Ack > c.sequence) {
				return ErrProtocol
			}
			if c.model.ValidateState(e.State) != nil {
				return ErrProtocol
			}
			if !old.exists {
				count++
			}
			old = stagedEntity{e.Generation, e.Owner, e.Ack, e.Dynamic, true}
		}
		staged[e.ID] = old
	}
	if count > MaxEntities {
		return ErrProtocol
	}
	for _, ch := range changes {
		if ch.Delete {
			delete(c.entities, ch.Entity.ID)
		} else {
			if err := c.update(ch.Entity, tick, true); err != nil {
				return err
			}
		}
	}
	c.revision = rev
	c.anchorTime(tick)
	return nil
}
func (c *Client) applySnapshot(b []byte) error {
	tick, rev, entities, err := decodeSnapshot(b)
	if err != nil {
		return err
	}
	if c.resyncing || rev != c.revision {
		return nil
	}
	applied := false
	for _, e := range entities {
		old := c.entities[e.ID]
		// Only reliable lifecycle messages may create an entity.
		if old == nil || !old.entity.Dynamic || old.entity.Generation != e.Generation || old.entity.Owner != e.Owner || old.entity.Dynamic != e.Dynamic || tick <= old.tick {
			continue
		}
		if err := c.update(e, tick, false); err != nil {
			return err
		}
		applied = true
	}
	if applied {
		c.anchorTime(tick)
	}
	return nil
}
func (c *Client) readReliable() {
	defer c.wg.Done()
	for {
		b, err := readFrame(c.stream, MaxFrame)
		if err != nil {
			c.fail(err)
			return
		}
		c.received.Add(uint64(len(b) + 4))
		c.mu.Lock()
		switch b[1] {
		case msgFull:
			err = c.applyFull(b)
		case msgChanges:
			err = c.applyChanges(b)
		case msgResult:
			d := decode(b, msgResult)
			id := d.u64()
			success := d.u8()
			data := d.bytes(MaxAction)
			err = d.end()
			if success > 1 {
				err = ErrProtocol
			}
			if err == nil {
				c.lastAction = max(c.lastAction, id)
			}
			if err == nil && id == c.pendingAction {
				reply := actionReply{id: id, data: append([]byte(nil), data...)}
				if success == 0 {
					reply.err = errors.New(string(data))
				}
				select {
				case c.replies <- reply:
				default:
				}
			}
		default:
			err = ErrProtocol
		}
		c.mu.Unlock()
		if err != nil {
			c.fail(err)
			return
		}
	}
}
func (c *Client) readDatagrams() {
	defer c.wg.Done()
	for {
		b, err := c.conn.ReceiveDatagram(c.conn.Context())
		if err != nil {
			c.fail(err)
			return
		}
		c.received.Add(uint64(len(b)))
		c.mu.Lock()
		err = c.applySnapshot(b)
		c.mu.Unlock()
		if err != nil {
			c.fail(err)
			return
		}
	}
}
func (c *Client) writeReliable() {
	defer c.wg.Done()
	for {
		select {
		case <-c.Done():
			return
		case b := <-c.reliable:
			c.stream.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if err := writeFrame(c.stream, b); err != nil {
				c.fail(err)
				return
			}
			c.sent.Add(uint64(len(b) + 4))
		}
	}
}
func (c *Client) writeDatagrams() {
	defer c.wg.Done()
	for {
		select {
		case <-c.Done():
			return
		case b := <-c.outgoing:
			if err := c.conn.SendDatagram(b); err != nil {
				c.fail(err)
				return
			}
			c.sent.Add(uint64(len(b)))
		}
	}
}
