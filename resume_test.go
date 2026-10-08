package statesync_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/arena"
	"github.com/qihai-coding/go-statesync/internal/nettest"
	"github.com/quic-go/quic-go"
)

func resume(t *testing.T, f fixture, old *syncnet.Client) *syncnet.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := syncnet.DialResume(ctx, f.s.Addr().String(), "alpha", f.tls, arena.Model{}, old.ResumeToken())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	if c.Player() != old.Player() {
		t.Fatal("identity changed")
	}
	return c
}
func TestResumeWeakNetworkMatrix(t *testing.T) {
	for _, rtt := range []time.Duration{0, 150 * time.Millisecond, 300 * time.Millisecond} {
		t.Run(rtt.String(), func(t *testing.T) {
			f := setup(t)
			profile := nettest.Profile{RTT: rtt, Jitter: 30 * time.Millisecond, Loss: .05, Duplicate: .02, Reorder: .02}
			p, err := nettest.New(f.s.Addr().String(), 912, profile)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			c := connect(t, f, p.Addr(), "alpha")
			ticker := time.NewTicker(time.Second / 30)
			defer ticker.Stop()
			for range 20 {
				<-ticker.C
				if err = c.SubmitInput(arena.Move(1, 0)); err != nil {
					t.Fatal(err)
				}
			}
			token := c.ResumeToken()
			id := c.Player()
			c.Disconnect()
			q, err := nettest.New(f.s.Addr().String(), 913, profile)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			next, err := syncnet.DialResume(ctx, q.Addr(), "alpha", f.tls, arena.Model{}, token)
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close()
			if next.Player() != id {
				t.Fatal("identity lost")
			}
			q.Set(nettest.Profile{})
			for range 30 {
				<-ticker.C
				if err = next.SubmitInput(arena.Move(0, 0)); err != nil {
					t.Fatal(err)
				}
			}
			view, _ := inspect(t, f.r)
			got := own(next)
			for _, e := range view.Entities {
				if e.Owner == id {
					predicted, ok := next.Sample(e.ID, time.Now())
					if !ok || !bytes.Equal(e.State, got.State) || !bytes.Equal(e.State, predicted) {
						t.Fatal("resumed state did not converge")
					}
				}
			}
		})
	}
}
func rejected(t *testing.T, f fixture, room string, token syncnet.ResumeToken) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := syncnet.DialResume(ctx, f.s.Addr().String(), room, f.tls, arena.Model{}, token)
	if c != nil {
		c.Close()
	}
	if !errors.Is(err, syncnet.ErrResumeRejected) {
		t.Fatalf("wanted resume rejection: %v", err)
	}
}
func TestResumeTakeoverAndOperationReplay(t *testing.T) {
	f := setup(t)
	c := connect(t, f, f.s.Addr().String(), "alpha")
	before := own(c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Action(ctx, 1, arena.Pickup(1, 1)); err != nil {
		t.Fatal(err)
	}
	next := resume(t, f, c)
	select {
	case <-c.Done():
	case <-ctx.Done():
		t.Fatal("old connection survived")
	}
	if !errors.Is(c.Err(), syncnet.ErrSessionReplaced) {
		t.Fatal(c.Err())
	}
	if next.LastActionID() != 1 || next.ResumeToken() != c.ResumeToken() {
		t.Fatal("session metadata lost")
	}
	after := own(next)
	if before.ID != after.ID || before.Generation != after.Generation {
		t.Fatal("entity replaced")
	}
	if _, err := next.Action(ctx, 1, arena.Pickup(1, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := next.Action(ctx, 1, arena.Pickup(2, 1)); err == nil {
		t.Fatal("same ID with different payload accepted")
	}
	state, _ := arena.Decode(own(next).State)
	if state.Pickups != 1 {
		t.Fatal("operation replayed")
	}
	c.Close()
	if err := next.SubmitInput(arena.Move(1, 0)); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return own(next).Ack > after.Ack })
}
func TestClosedRoomRejectsResume(t *testing.T) {
	f := setup(t)
	c := connect(t, f, f.s.Addr().String(), "alpha")
	token := c.ResumeToken()
	f.r.Close()
	rejected(t, f, "alpha", token)
}
func TestResumeConcurrentTakeovers(t *testing.T) {
	f := setup(t)
	old := connect(t, f, f.s.Addr().String(), "alpha")
	var wg sync.WaitGroup
	results := make(chan *syncnet.Client, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, _ := syncnet.DialResume(ctx, f.s.Addr().String(), "alpha", f.tls, arena.Model{}, old.ResumeToken())
			if c != nil {
				results <- c
			}
		}()
	}
	wg.Wait()
	close(results)
	var clients []*syncnet.Client
	for c := range results {
		clients = append(clients, c)
		t.Cleanup(c.Close)
	}
	if len(clients) == 0 {
		t.Fatal("no takeover succeeded")
	}
	eventually(t, time.Second, func() bool {
		alive := 0
		for _, c := range clients {
			select {
			case <-c.Done():
			default:
				alive++
			}
		}
		return alive == 1
	})
	_, m := inspect(t, f.r)
	if m.Players != 1 || m.RetainedPlayers != 0 {
		t.Fatal(m)
	}
}

type countedGame struct {
	*arena.Game
	leaves      atomic.Int32
	calls       atomic.Int32
	entered     chan struct{}
	release     chan struct{}
	panicStep   atomic.Bool
	invalidStep atomic.Bool
	panicInit   bool
	panicLeave  bool
}

func (g *countedGame) Leave(id uint32) []syncnet.Change {
	g.leaves.Add(1)
	if g.panicLeave {
		panic("injected leave failure")
	}
	return g.Game.Leave(id)
}
func TestRoomShutdownCleansSessionsOnce(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "cleanup panic"}[broken], func(t *testing.T) {
			g := &countedGame{Game: arena.New(), panicLeave: broken}
			f := configured(t, syncnet.DefaultConfig(), g)
			c := connect(t, f, f.s.Addr().String(), "alpha")
			connect(t, f, f.s.Addr().String(), "alpha")
			c.Disconnect()
			eventually(t, time.Second, func() bool { _, m := inspect(t, f.r); return m.RetainedPlayers == 1 })
			f.r.Close()
			f.r.Close()
			if g.leaves.Load() != 2 || (f.r.Err() != nil) != broken {
				t.Fatal("cleanup count or reason", g.leaves.Load(), f.r.Err())
			}
			rejected(t, f, "alpha", c.ResumeToken())
		})
	}
}
func (g *countedGame) Action(id uint32, b []byte) ([]syncnet.Change, []byte, error) {
	g.calls.Add(1)
	if g.entered != nil {
		select {
		case g.entered <- struct{}{}:
		default:
		}
		<-g.release
	}
	return g.Game.Action(id, b)
}
func (g *countedGame) Step(dt float32, in []syncnet.PlayerInput) []syncnet.Change {
	if g.invalidStep.Load() {
		return []syncnet.Change{
			{Entity: syncnet.Entity{ID: 999, Generation: 1, State: []byte{0}}},
			{Entity: syncnet.Entity{ID: 0, Generation: 1}},
		}
	}
	if g.panicStep.Load() {
		panic("injected game failure")
	}
	return g.Game.Step(dt, in)
}
func (g *countedGame) Entities() []syncnet.Entity {
	if g.panicInit {
		panic("injected init failure")
	}
	return g.Game.Entities()
}
func configured(t *testing.T, cfg syncnet.Config, g syncnet.Game) fixture {
	t.Helper()
	cert, pem, err := syncnet.LocalCertificate()
	if err != nil {
		t.Fatal(err)
	}
	tc, err := syncnet.ClientTLS(pem, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	s, err := syncnet.Listen("127.0.0.1:0", syncnet.ServerTLS(cert), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	r, err := s.CreateRoom("alpha", g)
	if err != nil {
		t.Fatal(err)
	}
	return fixture{s, r, tc}
}
func TestResumeExpiryCapacityAndLeave(t *testing.T) {
	cfg := syncnet.DefaultConfig()
	cfg.MaxPlayers = 1
	cfg.ResumeGracePeriod = 400 * time.Millisecond
	g := &countedGame{Game: arena.New()}
	f := configured(t, cfg, g)
	c := connect(t, f, f.s.Addr().String(), "alpha")
	token := c.ResumeToken()
	c.Disconnect()
	eventually(t, time.Second, func() bool { _, m := inspect(t, f.r); return m.Players == 0 && m.RetainedPlayers == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	extra, err := syncnet.Dial(ctx, f.s.Addr().String(), "alpha", f.tls, arena.Model{})
	if extra != nil {
		extra.Close()
		t.Fatal("reserved slot reused", err)
	}
	c2 := resume(t, f, c)
	c2.Disconnect()
	eventually(t, 2*time.Second, func() bool { _, m := inspect(t, f.r); return m.Players == 0 && m.RetainedPlayers == 0 })
	rejected(t, f, "alpha", token)
	if g.leaves.Load() != 1 {
		t.Fatal("expiry leave count", g.leaves.Load())
	}
	c3 := connect(t, f, f.s.Addr().String(), "alpha")
	c3.Close()
	eventually(t, time.Second, func() bool { _, m := inspect(t, f.r); return m.Players == 0 && m.RetainedPlayers == 0 })
	rejected(t, f, "alpha", c3.ResumeToken())
	if g.leaves.Load() != 2 {
		t.Fatal("explicit leave count", g.leaves.Load())
	}
}
func TestResumeCredentialsAndRestart(t *testing.T) {
	f := setup(t)
	c := connect(t, f, f.s.Addr().String(), "alpha")
	if _, err := f.s.CreateRoom("beta", arena.New()); err != nil {
		t.Fatal(err)
	}
	bad := c.ResumeToken()
	bad[0] ^= 255
	rejected(t, f, "alpha", bad)
	rejected(t, f, "beta", c.ResumeToken())
	rejected(t, f, "missing", c.ResumeToken())
	token := c.ResumeToken()
	f.s.Close()
	fresh := setup(t)
	rejected(t, fresh, "alpha", token)
	cfg := syncnet.DefaultConfig()
	cfg.ResumeGracePeriod = 0
	disabled := configured(t, cfg, arena.New())
	d := connect(t, disabled, disabled.s.Addr().String(), "alpha")
	if d.ResumeToken() != (syncnet.ResumeToken{}) {
		t.Fatal("disabled resumption issued credential")
	}
}
func TestActionCancellationDoesNotSend(t *testing.T) {
	g := &countedGame{Game: arena.New(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	f := configured(t, syncnet.DefaultConfig(), g)
	c := connect(t, f, f.s.Addr().String(), "alpha")
	var once sync.Once
	release := func() { once.Do(func() { close(g.release) }) }
	defer release()
	first := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := c.Action(ctx, 1, arena.Pickup(1, 1))
		first <- err
	}()
	select {
	case <-g.entered:
	case <-time.After(time.Second):
		t.Fatal("first action missing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := c.Action(ctx, 2, arena.Pickup(2, 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("cancellation blocked behind first action")
	}
	release()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	expired, stop := context.WithCancel(context.Background())
	stop()
	if _, err := c.Action(expired, 3, arena.Pickup(2, 1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if g.calls.Load() != 1 {
		t.Fatal("canceled operation sent")
	}
}
func TestConflictingRetryCannotConsumeLateReply(t *testing.T) {
	g := &countedGame{Game: arena.New(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	f := configured(t, syncnet.DefaultConfig(), g)
	c := connect(t, f, f.s.Addr().String(), "alpha")
	var once sync.Once
	release := func() { once.Do(func() { close(g.release) }) }
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := c.Action(ctx, 1, arena.Pickup(1, 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return g.calls.Load() == 1 })
	next, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err := c.Action(next, 1, arena.Pickup(2, 1)); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("conflicting retry was queued", err)
	}
	release()
	if _, err := c.Action(next, 1, arena.Pickup(1, 1)); err != nil {
		t.Fatal("identical retry failed", err)
	}
	if g.calls.Load() != 1 {
		t.Fatal("action repeated")
	}
}
func rawClient(t *testing.T, f fixture, window uint64) (*quic.Conn, *quic.Stream, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	qcfg := &quic.Config{EnableDatagrams: true}
	if window > 0 {
		qcfg.InitialStreamReceiveWindow = window
		qcfg.MaxStreamReceiveWindow = window
	}
	conn, err := quic.DialAddr(ctx, f.s.Addr().String(), f.tls, qcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseWithError(10, "test end") })
	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte{2, 1, 5, 0, 'a', 'l', 'p', 'h', 'a'}, make([]byte, 32)...)
	rawWrite(t, st, body)
	st.SetReadDeadline(time.Now().Add(3 * time.Second))
	var h [4]byte
	if _, err := io.ReadFull(st, h[:]); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, binary.LittleEndian.Uint32(h[:]))
	if _, err := io.ReadFull(st, b); err != nil {
		t.Fatal(err)
	}
	st.SetReadDeadline(time.Time{})
	return conn, st, b
}
func rawWrite(t *testing.T, st *quic.Stream, b []byte) {
	t.Helper()
	b = append(binary.LittleEndian.AppendUint32(nil, uint32(len(b))), b...)
	for len(b) > 0 {
		n, err := st.Write(b)
		if err != nil {
			t.Fatal(err)
		}
		b = b[n:]
	}
}
func TestResumeLostActionResult(t *testing.T) {
	f := setup(t)
	_, st, full := rawClient(t, f, 0)
	var token syncnet.ResumeToken
	copy(token[:], full[34:66])
	body := binary.LittleEndian.AppendUint64([]byte{2, 4}, 1)
	body = binary.LittleEndian.AppendUint16(body, 8)
	body = append(body, arena.Pickup(1, 1)...)
	rawWrite(t, st, body) // Deliberately never read the action result.
	eventually(t, time.Second, func() bool { v, _ := inspect(t, f.r); return len(v.Entities) == 16 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := syncnet.DialResume(ctx, f.s.Addr().String(), "alpha", f.tls, arena.Model{}, token)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.LastActionID() != 1 {
		t.Fatal("last action not restored")
	}
	if _, err = c.Action(ctx, 1, arena.Pickup(1, 1)); err != nil {
		t.Fatal(err)
	}
	state, _ := arena.Decode(own(c).State)
	if state.Pickups != 1 {
		t.Fatal("pickup executed twice")
	}
}
func TestRoomPanicIsolation(t *testing.T) {
	f := setup(t)
	healthy := connect(t, f, f.s.Addr().String(), "alpha")
	if _, err := f.s.CreateRoom("bad-init", &countedGame{Game: arena.New(), panicInit: true}); err == nil {
		t.Fatal("initialization panic escaped")
	}
	g := &countedGame{Game: arena.New()}
	bad, err := f.s.CreateRoom("bad-step", g)
	if err != nil {
		t.Fatal(err)
	}
	doomed := connect(t, f, f.s.Addr().String(), "bad-step")
	g.panicStep.Store(true)
	select {
	case <-bad.Done():
	case <-time.After(time.Second):
		t.Fatal("bad room still alive")
	}
	if bad.Err() == nil {
		t.Fatal("failure not observable")
	}
	select {
	case <-doomed.Done():
	case <-time.After(time.Second):
		t.Fatal("client not disconnected")
	}
	if err := healthy.SubmitInput(arena.Move(1, 0)); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return own(healthy).Ack == 1 })
	if f.r.Err() != nil {
		t.Fatal("healthy room affected")
	}
}
func TestInvalidLifecycleStopsOnlyItsRoom(t *testing.T) {
	f := setup(t)
	healthy := connect(t, f, f.s.Addr().String(), "alpha")
	g := &countedGame{Game: arena.New()}
	bad, err := f.s.CreateRoom("bad-batch", g)
	if err != nil {
		t.Fatal(err)
	}
	c := connect(t, f, f.s.Addr().String(), "bad-batch")
	g.invalidStep.Store(true)
	select {
	case <-bad.Done():
	case <-time.After(time.Second):
		t.Fatal("invalid batch did not terminate room")
	}
	if bad.Err() == nil || g.leaves.Load() != 1 {
		t.Fatal("missing reason or cleanup", bad.Err(), g.leaves.Load())
	}
	for _, e := range c.Authoritative().Entities {
		if e.ID == 999 {
			t.Fatal("partial batch published")
		}
	}
	if err := healthy.SubmitInput(arena.Move(1, 0)); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return own(healthy).Ack == 1 })
}
func TestResyncDelayChangeAndActualOutage(t *testing.T) {
	f := setup(t)
	p, err := nettest.New(f.s.Addr().String(), 123, nettest.Profile{RTT: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c := connect(t, f, p.Addr(), "alpha")
	if err = c.Resync(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(350 * time.Millisecond) // First response has reached the client.
	p.Set(nettest.Profile{})
	// A completed resync must retain a full second of cooldown, regardless of RTT.
	if err = c.Resync(); !errors.Is(err, syncnet.ErrBusy) {
		t.Fatal("cooldown absent", err)
	}
	time.Sleep(time.Second)
	if err = c.Resync(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	select {
	case <-c.Done():
		t.Fatal(c.Err())
	default:
	}
	p.Set(nettest.Profile{Loss: 1})
	select {
	case <-c.Done():
	case <-time.After(13 * time.Second):
		t.Fatal("actual outage not detected")
	}
	next := resume(t, f, c) // A new network route, without the failed proxy.
	if own(next).ID != own(c).ID {
		t.Fatal("outage replaced entity")
	}
}

func TestSlowClientIsolatedAndCannotResume(t *testing.T) {
	f := setup(t)
	healthy := connect(t, f, f.s.Addr().String(), "alpha")
	raw, st, full := rawClient(t, f, 1024)
	var token syncnet.ResumeToken
	copy(token[:], full[34:66])
	// Fill the peer's receive window without reading results; pace input to avoid
	// mistaking the room's incoming-queue limit for reliable-send backpressure.
	deadline := time.Now().Add(5 * time.Second)
	for id := uint64(1); time.Now().Before(deadline); id++ {
		select {
		case <-raw.Context().Done():
			goto closed
		default:
		}
		b := binary.LittleEndian.AppendUint64([]byte{2, 4}, id)
		b = binary.LittleEndian.AppendUint16(b, 8)
		b = append(b, arena.Pickup(999, 1)...)
		frame := append(binary.LittleEndian.AppendUint32(nil, uint32(len(b))), b...)
		st.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		if _, err := st.Write(frame); err != nil {
			break
		}
		time.Sleep(3 * time.Millisecond)
	}
closed:
	select {
	case <-raw.Context().Done():
	case <-time.After(3 * time.Second):
		t.Fatal("slow client retained indefinitely")
	}
	rejected(t, f, "alpha", token)
	if err := healthy.SubmitInput(arena.Move(1, 0)); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return own(healthy).Ack == 1 })
}

type capacityReplacementGame struct {
	*arena.Game
	extra []syncnet.Entity
}

func (g *capacityReplacementGame) Entities() []syncnet.Entity {
	return append(g.Game.Entities(), g.extra...)
}
func (g *capacityReplacementGame) Action(_ uint32, _ []byte) ([]syncnet.Change, []byte, error) {
	old := g.extra[0]
	g.extra[0].ID += 1000
	return []syncnet.Change{{Entity: g.extra[0]}, {Delete: true, Entity: old}}, []byte{1}, nil
}

func TestFullRoomLifecycleReplacementOverQUIC(t *testing.T) {
	g := &capacityReplacementGame{Game: arena.New()}
	initial := g.Game.Entities()
	for i := 0; i < syncnet.MaxEntities-len(initial)-1; i++ {
		e := initial[0]
		e.ID = uint32(1000 + i)
		g.extra = append(g.extra, e)
	}
	cfg := syncnet.DefaultConfig()
	cfg.MaxPlayers = 1
	f := configured(t, cfg, g)
	c := connect(t, f, f.s.Addr().String(), "alpha")
	if len(c.Authoritative().Entities) != syncnet.MaxEntities {
		t.Fatal("room not at capacity")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Action(ctx, 1, []byte{1}); err != nil {
		t.Fatal("valid replacement disconnected client", err)
	}
	view := c.Authoritative()
	found := false
	for _, e := range view.Entities {
		if e.ID == 1000 {
			t.Fatal("old entity survived")
		}
		found = found || e.ID == 2000
	}
	if !found || len(view.Entities) != syncnet.MaxEntities || c.Err() != nil {
		t.Fatal("replacement or connection lost", c.Err())
	}
	serverView, _ := inspect(t, f.r)
	if len(serverView.Entities) != len(view.Entities) {
		t.Fatal("server/client entity count differs")
	}
}

func TestServerRejectsOversizedRequestsFromPrefix(t *testing.T) {
	for _, joined := range []bool{false, true} {
		name := "handshake"
		length := uint32(101)
		if joined {
			name = "command"
			length = 12 + syncnet.MaxAction + 1
		}
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			healthy := connect(t, f, f.s.Addr().String(), "alpha")
			var raw *quic.Conn
			var st *quic.Stream
			var token syncnet.ResumeToken
			if joined {
				var full []byte
				raw, st, full = rawClient(t, f, 0)
				copy(token[:], full[34:66])
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				var err error
				raw, err = quic.DialAddr(ctx, f.s.Addr().String(), f.tls, &quic.Config{EnableDatagrams: true})
				if err != nil {
					t.Fatal(err)
				}
				defer raw.CloseWithError(10, "test end")
				st, err = raw.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
			}
			// Send only the length: the server must reject before requesting a body.
			if _, err := st.Write(binary.LittleEndian.AppendUint32(nil, length)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-raw.Context().Done():
			case <-time.After(500 * time.Millisecond):
				t.Fatal("oversized request prefix left server waiting for its body")
			}
			var app *quic.ApplicationError
			if !errors.As(context.Cause(raw.Context()), &app) || app.ErrorCode != 4 {
				t.Fatal("missing protocol rejection", context.Cause(raw.Context()))
			}
			if joined {
				rejected(t, f, "alpha", token)
			}
			if err := healthy.SubmitInput(arena.Move(1, 0)); err != nil {
				t.Fatal(err)
			}
			eventually(t, time.Second, func() bool { return own(healthy).Ack == 1 })
		})
	}
}

func TestLargestLegalClientFrames(t *testing.T) {
	f := setup(t)
	name := strings.Repeat("r", 64)
	if _, err := f.s.CreateRoom(name, arena.New()); err != nil {
		t.Fatal(err)
	}
	c := connect(t, f, f.s.Addr().String(), name)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// The demo game rejects this payload, but the frame must reach game logic.
	if _, err := c.Action(ctx, 1, make([]byte, syncnet.MaxAction)); err == nil || errors.Is(err, syncnet.ErrProtocol) {
		t.Fatal("expected game rejection without a framing error", err)
	}
	if c.LastActionID() != 1 || c.Err() != nil {
		t.Fatal("maximum action did not reach the server", c.Err())
	}
	if _, err := c.Action(ctx, 2, arena.Pickup(1, 1)); err != nil {
		t.Fatal("maximum action broke subsequent requests", err)
	}
}
