package statesync

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestProtocolGoldenAndBounds(t *testing.T) {
	b := encodeInputs([]Input{{Sequence: 1, Data: []byte{0x7f}}})
	if hex.EncodeToString(b) != "030701010000000000000001007f" {
		t.Fatalf("wire format changed: %x", b)
	}
	in, err := decodeInputs(b)
	if err != nil || len(in) != 1 || in[0].Sequence != 1 || in[0].Data[0] != 0x7f {
		t.Fatal(in, err)
	}
	for _, bad := range [][]byte{nil, {ProtocolVersion}, {2, 7, 1}, append(append([]byte(nil), b...), 0), {ProtocolVersion, 7, 0}, {ProtocolVersion, 7, 4}} {
		if _, err := decodeInputs(bad); err == nil {
			t.Fatalf("accepted %x", bad)
		}
	}
	var framed bytes.Buffer
	if err := writeFrame(&framed, b); err != nil {
		t.Fatal(err)
	}
	got, err := readFrame(&framed, MaxFrame)
	if err != nil || !bytes.Equal(got, b) {
		t.Fatal(err, got)
	}
	if _, err := readFrame(bytes.NewReader([]byte{255, 255, 255, 255}), MaxFrame); err == nil {
		t.Fatal("oversize frame accepted")
	}
	if err := writeAll(zeroWriter{}, []byte{1}); err != io.ErrShortWrite {
		t.Fatal(err)
	}
}

type zeroWriter struct{}

func TestFrameDirectionalLimits(t *testing.T) {
	for _, limit := range []uint32{maxJoinFrame, maxClientFrame, MaxFrame} {
		// A length-only rejection proves no body is needed to enforce the cap.
		header := le.AppendUint32(nil, limit+1)
		if _, err := readFrame(bytes.NewReader(header), limit); err != ErrProtocol {
			t.Fatalf("limit %d: oversized prefix returned %v", limit, err)
		}
		b := make([]byte, limit)
		b[0] = ProtocolVersion
		var framed bytes.Buffer
		if err := writeFrame(&framed, b); err != nil {
			t.Fatal(err)
		}
		got, err := readFrame(&framed, limit)
		if err != nil || !bytes.Equal(got, b) {
			t.Fatalf("limit %d: boundary frame rejected: %v", limit, err)
		}
	}
}

func TestSessionWireLayout(t *testing.T) {
	join := start(msgJoin)
	join.bytes([]byte("alpha"))
	join = append(join, make([]byte, 32)...)
	var framed bytes.Buffer
	if err := writeFrame(&framed, join); err != nil {
		t.Fatal(err)
	}
	want := "2900000003010500616c706861" + "0000000000000000000000000000000000000000000000000000000000000000"
	if hex.EncodeToString(framed.Bytes()) != want {
		t.Fatalf("join layout changed: %x", framed.Bytes())
	}
	cfg := DefaultConfig()
	session := sessionState{token: ResumeToken{1, 2, 3}, lastAction: 42, epoch: 11}
	v := View{Tick: 7, Revision: 8, ServerTime: 123 * time.Millisecond, Player: 9}
	b := encodeFull(v, cfg, session)
	if len(b) != 93 || le.Uint64(b[18:26]) != uint64(v.ServerTime) || le.Uint64(b[26:34]) != session.epoch ||
		b[42] != byte(DeltaSnapshots) || le.Uint64(b[43:51]) != uint64(time.Minute) ||
		!bytes.Equal(b[51:83], session.token[:]) || le.Uint64(b[83:91]) != 42 {
		t.Fatalf("full state session layout changed: %x", b)
	}
	got, decoded, meta, err := decodeFull(b)
	if err != nil || got.Player != v.Player || got.Tick != v.Tick || got.ServerTime != v.ServerTime ||
		decoded.ResumeGracePeriod != time.Minute || meta != session {
		t.Fatal("session round trip failed", err)
	}
}

func TestProtocolRejectsVersionTwo(t *testing.T) {
	b := encodeInputs([]Input{{Sequence: 1, Data: []byte{0}}})
	b[0] = 2
	if _, err := decodeInputs(b); err != ErrProtocol {
		t.Fatal("version two datagram accepted", err)
	}
	var framed bytes.Buffer
	if err := writeFrame(&framed, b); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrame(&framed, MaxFrame); err != ErrProtocol {
		t.Fatal("version two reliable frame accepted", err)
	}
}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }
func TestIndependentSnapshotChunks(t *testing.T) {
	es := make([]Entity, 40)
	for i := range es {
		es[i] = Entity{ID: uint32(i + 1), Generation: 1, Dynamic: true, State: make([]byte, 100)}
	}
	packets, err := snapshotPackets(8, 3, es, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(packets) < 2 {
		t.Fatal("expected split")
	}
	count := 0
	for _, p := range packets {
		if len(p) > 1000 {
			t.Fatal("datagram oversized")
		}
		tick, rev, chunk, err := decodeSnapshot(p)
		if err != nil || tick != 8 || rev != 3 {
			t.Fatal(err)
		}
		count += len(chunk)
	}
	if count != len(es) {
		t.Fatal("record lost")
	}
	if _, err := snapshotPackets(1, 1, []Entity{{ID: 1, Generation: 1, Dynamic: true, State: make([]byte, 513)}}, 1000); err == nil {
		t.Fatal("large state accepted")
	}
	boundary := Entity{ID: ^uint32(0), Generation: ^uint32(0), Dynamic: true, Ack: ^uint64(0), State: make([]byte, MaxState)}
	packets, err = snapshotPackets(1, 1, []Entity{boundary}, 600)
	if err != nil || len(packets) != 1 || len(packets[0]) > 600 {
		t.Fatal("maximum state did not fit minimum datagram", err)
	}
	_, _, decoded, err := decodeSnapshot(packets[0])
	if err != nil || len(decoded) != 1 || decoded[0].ID != boundary.ID || decoded[0].Generation != boundary.Generation ||
		decoded[0].Ack != boundary.Ack || !bytes.Equal(decoded[0].State, boundary.State) {
		t.Fatal("boundary record did not round trip", err)
	}
}

type byteModel struct{}

func (byteModel) ValidateInput(b []byte) error {
	if len(b) != 1 {
		return ErrProtocol
	}
	return nil
}
func (byteModel) ValidateState(b []byte) error {
	if len(b) != 1 {
		return ErrProtocol
	}
	return nil
}
func (byteModel) Predict(s, b []byte, dt float32) ([]byte, error) { return []byte{s[0] + b[0]}, nil }
func (byteModel) Interpolate(a, b []byte, f float32) []byte {
	return []byte{byte(float32(a[0]) + (float32(b[0])-float32(a[0]))*f)}
}
func testClient(t *testing.T) *Client {
	t.Helper()
	c := &Client{model: byteModel{}, buffer: 200 * time.Millisecond, reliable: make(chan []byte, 64)}
	es := []Entity{{ID: 1, Generation: 1, Owner: 1, Dynamic: true, State: []byte{0}}, {ID: 2, Generation: 1, Owner: 2, Dynamic: true, State: []byte{0}}}
	if err := c.applyFull(encodeFull(View{Player: 1, Revision: 1, Entities: es}, DefaultConfig(), sessionState{epoch: 1})); err != nil {
		t.Fatal(err)
	}
	return c
}
func TestReconciliationAndLifecycle(t *testing.T) {
	c := testClient(t)
	c.sequence = 2
	c.history = []Input{{1, []byte{1}}, {2, []byte{2}}}
	e := Entity{ID: 1, Generation: 1, Owner: 1, Dynamic: true, Ack: 1, State: []byte{5}}
	if err := c.update(e, 2, 2*time.Second/30, false); err != nil {
		t.Fatal(err)
	}
	if c.predicted[0] != 7 || len(c.history) != 1 {
		t.Fatal("unacknowledged input was not replayed")
	}
	deleted := Entity{ID: 2, Generation: 1, State: []byte{0}}
	if err := c.applyChanges(encodeChanges(3, 2, 100*time.Millisecond, []Change{{Delete: true, Entity: deleted}})); err != nil {
		t.Fatal(err)
	}
	old := Entity{ID: 2, Generation: 1, Owner: 2, Dynamic: true, State: []byte{99}}
	packets, _ := snapshotPackets(100, 1, []Entity{old}, 1000)
	if err := c.applySnapshot(packets[0]); err != nil {
		t.Fatal(err)
	}
	if c.entities[2] != nil {
		t.Fatal("late packet resurrected entity")
	}
	replacement := old
	replacement.Generation = 2
	replacement.State = []byte{10}
	if err := c.applyChanges(encodeChanges(4, 3, 4*time.Second/30, []Change{{Entity: replacement}})); err != nil {
		t.Fatal(err)
	}
	packets, _ = snapshotPackets(101, 3, []Entity{old}, 1000)
	c.applySnapshot(packets[0])
	if c.entities[2].entity.State[0] != 10 {
		t.Fatal("old generation overwrote replacement")
	}
}
func TestInterpolationHoldsAtEdges(t *testing.T) {
	c := testClient(t)
	c.update(Entity{ID: 2, Generation: 1, Owner: 2, Dynamic: true, State: []byte{30}}, 30, time.Second, false)
	now := time.Now()
	c.anchor = 30
	c.anchorAt = now
	b, ok := c.Sample(2, now)
	if !ok || b[0] != 24 {
		t.Fatal(b, ok)
	}
	b, _ = c.Sample(2, now.Add(time.Hour))
	if b[0] != 30 {
		t.Fatal("unexpected extrapolation")
	}
	b, _ = c.Sample(2, now.Add(-time.Hour))
	if b[0] != 0 {
		t.Fatal("unexpected back extrapolation")
	}
}
func TestSnapshotDuplicatesAndReordering(t *testing.T) {
	c := testClient(t)
	e := Entity{ID: 2, Generation: 1, Owner: 2, Dynamic: true, State: []byte{20}}
	for _, frame := range []struct {
		tick  uint64
		value byte
	}{{10, 20}, {10, 99}, {8, 88}, {12, 30}} {
		e.State = []byte{frame.value}
		packets, err := snapshotPackets(frame.tick, 1, []Entity{e}, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.applySnapshot(packets[0]); err != nil {
			t.Fatal(err)
		}
		want := byte(20)
		if frame.tick == 12 {
			want = 30
		}
		if c.entities[2].entity.State[0] != want {
			t.Fatal("duplicate or older snapshot replaced newer state")
		}
	}
}

func TestIgnoredSnapshotDoesNotAdvanceClock(t *testing.T) {
	for _, reason := range []string{"old generation", "unknown entity", "static entity"} {
		t.Run(reason, func(t *testing.T) {
			c := testClient(t)
			c.entities[2].entity.Generation = 2
			if reason == "static entity" {
				c.entities[2].entity.Dynamic = false
			}
			e := c.entities[2].entity
			e.State = []byte{99}
			switch reason {
			case "old generation":
				e.Generation--
			case "unknown entity":
				e.ID = 999
			}
			// Static status comes from the reliable lifecycle, not snapshot records.
			e.Dynamic = true
			packets, err := snapshotPackets(100, c.revision, []Entity{e}, 1000)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.applySnapshot(packets[0]); err != nil {
				t.Fatal(err)
			}
			if c.tick != 0 || c.anchor != 0 || c.entities[2].entity.State[0] != 0 {
				t.Error("ignored snapshot changed state or interpolation clock")
			}
			current := c.entities[2].entity
			current.State = []byte{7}
			if err := c.applyChanges(encodeChanges(1, 2, time.Second/30, []Change{{Entity: current}})); err != nil {
				t.Fatal("ignored snapshot invalidated a later reliable batch", err)
			}
		})
	}
}

func TestMixedSnapshotAdvancesFromAcceptedRecord(t *testing.T) {
	c := testClient(t)
	unknown := Entity{ID: 999, Generation: 1, Owner: 2, Dynamic: true, State: []byte{99}}
	valid := c.entities[2].entity
	valid.State = []byte{7}
	packets, err := snapshotPackets(15, c.revision, []Entity{unknown, valid}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.applySnapshot(packets[0]); err != nil {
		t.Fatal(err)
	}
	if c.tick != 15 || c.anchor != 15 || c.entities[2].entity.State[0] != 7 || c.entities[999] != nil {
		t.Fatal("valid independent record was not applied")
	}
}
func FuzzDecode(f *testing.F) {
	f.Add(encodeInputs([]Input{{1, []byte{0}}}))
	f.Add([]byte{ProtocolVersion, msgSnapshot})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxFrame {
			return
		}
		decodeInputs(b)
		decodeSnapshot(b)
		decodeFull(b)
		decodeChanges(b)
	})
}

func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{2, 0, 0, 0, ProtocolVersion, msgResync})
	f.Add(le.AppendUint32(nil, maxJoinFrame+1))
	f.Add(le.AppendUint32(nil, maxClientFrame+1))
	f.Add([]byte{255, 255, 255, 255})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxFrame+4 {
			return
		}
		for _, limit := range []uint32{maxJoinFrame, maxClientFrame, MaxFrame, ^uint32(0)} {
			got, err := readFrame(bytes.NewReader(b), limit)
			if err == nil {
				if len(got) < 2 || uint32(len(got)) > min(limit, MaxFrame) || got[0] != ProtocolVersion {
					t.Fatal("accepted invalid frame")
				}
				if !bytes.Equal(got, b[4:4+len(got)]) {
					t.Fatal("frame body changed")
				}
			}
		}
	})
}
func BenchmarkSnapshotEncoding(b *testing.B) {
	es := make([]Entity, 16)
	for i := range es {
		es[i] = Entity{ID: uint32(i + 1), Generation: 1, Owner: uint32(i + 1), Dynamic: true, State: make([]byte, 13)}
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := snapshotPackets(1, 1, es, 1000); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLifecycleBatchApply(b *testing.B) {
	for _, count := range []int{16, MaxEntities} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			c := &Client{model: byteModel{}, reliable: make(chan []byte, 64)}
			entities := make([]Entity, count)
			for i := range entities {
				entities[i] = Entity{ID: uint32(i + 1), Generation: 1, Owner: uint32(i + 1), Dynamic: true, State: []byte{0}}
			}
			if err := c.applyFull(encodeFull(View{Player: 1, Revision: 1, Entities: entities}, DefaultConfig(), sessionState{epoch: 1})); err != nil {
				b.Fatal(err)
			}
			packet := encodeChanges(0, 0, 0, []Change{{Entity: entities[1]}})
			b.ReportAllocs()
			for b.Loop() {
				le.PutUint64(packet[2:10], c.tick+1)
				le.PutUint64(packet[10:18], c.revision+1)
				le.PutUint64(packet[18:26], uint64(time.Duration(c.tick+1)*time.Second/30))
				if err := c.applyChanges(packet); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type vectorModel struct{ byteModel }

func (vectorModel) ValidateState(b []byte) error {
	if len(b) == 0 || len(b) > MaxState {
		return ErrProtocol
	}
	return nil
}
func (vectorModel) Predict(s, in []byte, _ float32) ([]byte, error) {
	b := append([]byte(nil), s...)
	b[0] += in[0]
	return b, nil
}

func vectorClient(t *testing.T) (*Client, replication) {
	t.Helper()
	v := View{Player: 1, Revision: 1, Entities: []Entity{
		{ID: 1, Generation: 1, Owner: 1, Dynamic: true, State: make([]byte, 32)},
		{ID: 2, Generation: 1, Owner: 2, Dynamic: true, State: make([]byte, 32)},
	}}
	c := &Client{model: vectorModel{}, buffer: 200 * time.Millisecond}
	r := replication{}
	epoch, err := r.installFull(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.applyFull(encodeFull(v, DefaultConfig(), sessionState{epoch: epoch})); err != nil {
		t.Fatal(err)
	}
	if err := r.acknowledge(epoch, v.Revision, true); err != nil {
		t.Fatal(err)
	}
	return c, r
}

func clientDeltaPacket(t *testing.T, r *replication, tick uint64, e Entity) []byte {
	t.Helper()
	groups, err := snapshotGroups([]Entity{e}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	v := View{Tick: tick, Revision: r.revision, ServerTime: time.Duration(tick) * time.Second / 30}
	packets, stats, err := r.build(v, groups, DeltaSnapshots)
	if err != nil || len(packets) != 1 || stats.deltaRecords != 1 {
		t.Fatal("fixture did not produce a delta", err, stats)
	}
	return packets[0]
}

func TestClientDeltaSnapshotsDoNotDependOnPreviousPacket(t *testing.T) {
	c, r := vectorClient(t)
	e := cloneEntity(c.entities[2].entity)
	e.State[0] = 5
	lost := clientDeltaPacket(t, &r, 1, e)
	e.State[0] = 9
	latest := clientDeltaPacket(t, &r, 3, e)
	for _, packet := range [][]byte{latest, lost, latest} {
		if err := c.applySnapshot(packet); err != nil {
			t.Fatal(err)
		}
		if c.entities[2].entity.State[0] != 9 || c.entities[2].tick != 3 || c.bases[2].state[0] != 0 {
			t.Fatal("loss, reordering or duplicate changed the independent baseline")
		}
	}
}

func TestClientDeltaUnchangedStateStillAcknowledgesInputs(t *testing.T) {
	c, r := vectorClient(t)
	c.sequence = 2
	c.history = []Input{{Sequence: 1, Data: []byte{1}}, {Sequence: 2, Data: []byte{2}}}
	c.predicted[0] = 3
	e := c.entities[1].entity
	e.Ack = 1
	if err := c.applySnapshot(clientDeltaPacket(t, &r, 1, e)); err != nil {
		t.Fatal(err)
	}
	if c.entities[1].entity.Ack != 1 || len(c.history) != 1 || c.history[0].Sequence != 2 || c.predicted[0] != 2 {
		t.Fatal("unchanged state skipped input confirmation")
	}
}

func TestClientFullFallbackKeepsReliableBaseline(t *testing.T) {
	c, r := vectorClient(t)
	e := cloneEntity(c.entities[2].entity)
	e.State = []byte{9}
	groups, err := snapshotGroups([]Entity{e}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	packets, stats, err := r.build(View{Tick: 1, Revision: 1, ServerTime: time.Second / 30}, groups, DeltaSnapshots)
	if err != nil || stats.fullRecords != 1 || len(packets) != 1 {
		t.Fatal("length change did not use complete record", err, stats)
	}
	if err := c.applySnapshot(packets[0]); err != nil || len(c.entities[2].entity.State) != 1 || len(c.bases[2].state) != 32 {
		t.Fatal("complete fallback replaced reliable baseline", err)
	}
	e.State = make([]byte, 32)
	e.State[0] = 12
	if err := c.applySnapshot(clientDeltaPacket(t, &r, 2, e)); err != nil || len(c.entities[2].entity.State) != 32 || c.entities[2].entity.State[0] != 12 {
		t.Fatal("delta could not restore original-length baseline", err)
	}
}

func TestClientResyncChangesEpochAtSameRevision(t *testing.T) {
	c, r := vectorClient(t)
	e := cloneEntity(c.entities[2].entity)
	e.State[0] = 7
	old := clientDeltaPacket(t, &r, 100, e)
	c.resyncing = true
	v := c.Authoritative()
	epoch, err := r.installFull(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.applyFull(encodeFull(v, DefaultConfig(), sessionState{epoch: epoch})); err != nil {
		t.Fatal(err)
	}
	if err := c.applySnapshot(old); err != nil {
		t.Fatal(err)
	}
	if c.tick != 0 || c.entities[2].entity.State[0] != 0 || c.epoch != 2 {
		t.Fatal("old epoch crossed a same-revision resync")
	}
	if err := r.acknowledge(epoch, v.Revision, true); err != nil {
		t.Fatal(err)
	}
	if err := c.applySnapshot(clientDeltaPacket(t, &r, 1, e)); err != nil || c.entities[2].entity.State[0] != 7 {
		t.Fatal("new epoch delta did not apply", err)
	}
}

func TestClientReliableLifecycleOwnsDeltaBaseline(t *testing.T) {
	c, r := vectorClient(t)
	e := cloneEntity(c.entities[2].entity)
	apply := func(tick, revision uint64, changes []Change) {
		t.Helper()
		if err := c.applyChanges(encodeChanges(tick, revision, time.Duration(tick)*time.Second/30, changes)); err != nil {
			t.Fatal(err)
		}
		r.installChanges(changes, revision)
		if err := r.acknowledge(r.epoch, revision, true); err != nil {
			t.Fatal(err)
		}
	}
	e.State[0] = 10
	apply(1, 2, []Change{{Entity: e}})
	if c.bases[2].state[0] != 0 {
		t.Fatal("ordinary reliable update replaced immutable baseline")
	}
	e.State = append([]byte(nil), e.State...)
	e.State[0] = 20
	if err := c.applySnapshot(clientDeltaPacket(t, &r, 2, e)); err != nil || c.entities[2].entity.State[0] != 20 {
		t.Fatal("delta used ordinary update as baseline", err)
	}
	e.Generation, e.Owner = 2, 3
	e.State = append([]byte(nil), e.State...)
	e.State[0] = 50
	apply(3, 3, []Change{{Entity: e}})
	if c.bases[2].generation != 2 || c.bases[2].state[0] != 50 {
		t.Fatal("new generation did not install reliable baseline")
	}
	e.State = append([]byte(nil), e.State...)
	e.State[0] = 55
	late := clientDeltaPacket(t, &r, 4, e)
	if err := c.applySnapshot(late); err != nil || c.entities[2].entity.State[0] != 55 || c.entities[2].entity.Owner != 3 {
		t.Fatal("replacement delta lost lifecycle metadata", err)
	}
	apply(5, 4, []Change{{Delete: true, Entity: e}})
	if _, ok := c.bases[2]; ok {
		t.Fatal("deleted entity retained its baseline")
	}
	if err := c.applySnapshot(late); err != nil || c.entities[2] != nil {
		t.Fatal("late delta resurrected deleted entity", err)
	}
}
