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
	if hex.EncodeToString(b) != "020701010000000000000001007f" {
		t.Fatalf("wire format changed: %x", b)
	}
	in, err := decodeInputs(b)
	if err != nil || len(in) != 1 || in[0].Sequence != 1 || in[0].Data[0] != 0x7f {
		t.Fatal(in, err)
	}
	for _, bad := range [][]byte{nil, {2}, {1, 7, 1}, append(append([]byte(nil), b...), 0), {2, 7, 0}, {2, 7, 4}} {
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
	want := "2900000002010500616c706861" + "0000000000000000000000000000000000000000000000000000000000000000"
	if hex.EncodeToString(framed.Bytes()) != want {
		t.Fatalf("join layout changed: %x", framed.Bytes())
	}
	cfg := DefaultConfig()
	session := sessionState{token: ResumeToken{1, 2, 3}, lastAction: 42}
	v := View{Tick: 7, Revision: 8, Player: 9}
	b := encodeFull(v, cfg, session)
	if len(b) != 76 || le.Uint64(b[26:34]) != uint64(time.Minute) ||
		!bytes.Equal(b[34:66], session.token[:]) || le.Uint64(b[66:74]) != 42 {
		t.Fatalf("full state session layout changed: %x", b)
	}
	got, decoded, meta, err := decodeFull(b)
	if err != nil || got.Player != v.Player || got.Tick != v.Tick ||
		decoded.ResumeGracePeriod != time.Minute || meta != session {
		t.Fatal("session round trip failed", err)
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
	c := &Client{model: byteModel{}, buffer: 200 * time.Millisecond}
	es := []Entity{{ID: 1, Generation: 1, Owner: 1, Dynamic: true, State: []byte{0}}, {ID: 2, Generation: 1, Owner: 2, Dynamic: true, State: []byte{0}}}
	if err := c.applyFull(encodeFull(View{Player: 1, Revision: 1, Entities: es}, DefaultConfig(), sessionState{})); err != nil {
		t.Fatal(err)
	}
	return c
}
func TestReconciliationAndLifecycle(t *testing.T) {
	c := testClient(t)
	c.sequence = 2
	c.history = []Input{{1, []byte{1}}, {2, []byte{2}}}
	e := Entity{ID: 1, Generation: 1, Owner: 1, Dynamic: true, Ack: 1, State: []byte{5}}
	if err := c.update(e, 2, false); err != nil {
		t.Fatal(err)
	}
	if c.predicted[0] != 7 || len(c.history) != 1 {
		t.Fatal("unacknowledged input was not replayed")
	}
	deleted := Entity{ID: 2, Generation: 1, State: []byte{0}}
	if err := c.applyChanges(encodeChanges(3, 2, []Change{{Delete: true, Entity: deleted}})); err != nil {
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
	if err := c.applyChanges(encodeChanges(4, 3, []Change{{Entity: replacement}})); err != nil {
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
	c.update(Entity{ID: 2, Generation: 1, Owner: 2, Dynamic: true, State: []byte{30}}, 30, false)
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
	for _, reason := range []string{"old generation", "wrong owner", "unknown entity", "static entity"} {
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
			case "wrong owner":
				e.Owner++
			case "unknown entity":
				e.ID = 999
			}
			packet := stateHeader(msgSnapshot, 100, c.revision)
			packet.u16(1)
			packet.entity(e)
			if err := c.applySnapshot(packet); err != nil {
				t.Fatal(err)
			}
			if c.tick != 0 || c.anchor != 0 || c.entities[2].entity.State[0] != 0 {
				t.Error("ignored snapshot changed state or interpolation clock")
			}
			current := c.entities[2].entity
			current.State = []byte{7}
			if err := c.applyChanges(encodeChanges(1, 2, []Change{{Entity: current}})); err != nil {
				t.Fatal("ignored snapshot invalidated a later reliable batch", err)
			}
		})
	}
}

func TestMixedSnapshotAdvancesFromAcceptedRecord(t *testing.T) {
	c := testClient(t)
	packet := stateHeader(msgSnapshot, 15, c.revision)
	packet.u16(2)
	unknown := Entity{ID: 999, Generation: 1, Owner: 2, Dynamic: true, State: []byte{99}}
	packet.entity(unknown)
	valid := c.entities[2].entity
	valid.State = []byte{7}
	packet.entity(valid)
	if err := c.applySnapshot(packet); err != nil {
		t.Fatal(err)
	}
	if c.tick != 15 || c.anchor != 15 || c.entities[2].entity.State[0] != 7 || c.entities[999] != nil {
		t.Fatal("valid independent record was not applied")
	}
}
func FuzzDecode(f *testing.F) {
	f.Add(encodeInputs([]Input{{1, []byte{0}}}))
	f.Add([]byte{2, 8})
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
			c := &Client{model: byteModel{}}
			entities := make([]Entity, count)
			for i := range entities {
				entities[i] = Entity{ID: uint32(i + 1), Generation: 1, Owner: uint32(i + 1), Dynamic: true, State: []byte{0}}
			}
			if err := c.applyFull(encodeFull(View{Player: 1, Revision: 1, Entities: entities}, DefaultConfig(), sessionState{})); err != nil {
				b.Fatal(err)
			}
			packet := encodeChanges(0, 0, []Change{{Entity: entities[1]}})
			b.ReportAllocs()
			for b.Loop() {
				le.PutUint64(packet[2:10], c.tick+1)
				le.PutUint64(packet[10:18], c.revision+1)
				if err := c.applyChanges(packet); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
