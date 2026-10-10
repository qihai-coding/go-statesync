package statesync

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestDeltaRoundTripAndFallback(t *testing.T) {
	for _, size := range []int{1, 7, 8, 9, 32, MaxState} {
		base := make([]byte, size)
		state := append([]byte(nil), base...)
		state[size-1] = 7
		payload, delta := stateDelta(state, base)
		if delta {
			got, err := restoreDelta(payload, base)
			if err != nil || !bytes.Equal(got, state) || !bytes.Equal(base, make([]byte, size)) {
				t.Fatalf("size %d: round trip=%v err=%v", size, got, err)
			}
		} else if !bytes.Equal(payload, state) {
			t.Fatalf("size %d: full fallback changed state", size)
		}
	}
	for _, pair := range [][2][]byte{{nil, nil}, {[]byte{1}, nil}, {[]byte{1, 1, 1}, []byte{0, 0, 0}}} {
		payload, delta := stateDelta(pair[0], pair[1])
		if delta || !bytes.Equal(payload, pair[0]) {
			t.Fatal("length change or non-beneficial delta did not use full state")
		}
	}
	base := make([]byte, 9)
	for _, invalid := range [][]byte{nil, {0}, {0, 2}, {1, 0}, {1, 0, 0}, {0, 0, 3}, {1, 0, 3, 4}, make([]byte, 9)} {
		if _, err := restoreDelta(invalid, base); !errors.Is(err, ErrProtocol) {
			t.Fatalf("invalid bitmap %x accepted", invalid)
		}
	}
}

func TestCanonicalSnapshotVarints(t *testing.T) {
	for _, value := range []uint64{0, 1, 127, 128, 255, 1 << 32, ^uint64(0)} {
		var b encoder
		b.var64(value)
		d := &decoder{b: b}
		if got := d.var64(); got != value || d.end() != nil {
			t.Fatalf("value %d decoded as %d", value, got)
		}
	}
	for _, invalid := range [][]byte{{0x80}, {0x80, 0}, {0x81, 0}, {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 2}, bytes.Repeat([]byte{0x80}, 11)} {
		d := &decoder{b: invalid}
		d.var64()
		if !errors.Is(d.end(), ErrProtocol) {
			t.Fatalf("invalid varint %x accepted", invalid)
		}
	}
	var overflow encoder
	overflow.var64(1 << 32)
	d := &decoder{b: overflow}
	d.var32()
	if !errors.Is(d.end(), ErrProtocol) {
		t.Fatal("32-bit overflow accepted")
	}
}

func TestReplicationDictionaryConfirmation(t *testing.T) {
	e := Entity{ID: 1, Generation: 1, Dynamic: true, State: make([]byte, 32)}
	v := View{Tick: 1, Revision: 3, Entities: []Entity{e}, ServerTime: time.Second}
	var r replication
	epoch, err := r.installFull(v)
	if err != nil || epoch != 1 {
		t.Fatalf("install full: epoch=%d err=%v", epoch, err)
	}
	if &r.bases[1].state[0] != &e.State[0] {
		t.Fatal("dictionary copied immutable game state")
	}
	groups, err := snapshotGroups(v.Entities, 600)
	if err != nil {
		t.Fatal(err)
	}
	assertDelta := func(want bool) {
		t.Helper()
		packets, _, err := r.build(v, groups, DeltaSnapshots)
		if err != nil || len(packets) != 1 {
			t.Fatalf("build: packets=%d err=%v", len(packets), err)
		}
		p, err := decodePacket(packets[0])
		if err != nil || p.records[0].delta != want {
			t.Fatalf("want delta=%v packet=%+v err=%v", want, p, err)
		}
	}
	assertDelta(false)
	if r.acknowledge(2, 3, true) == nil || r.acknowledge(1, 4, true) == nil {
		t.Fatal("future dictionary confirmation accepted")
	}
	if err := r.acknowledge(1, 3, true); err != nil {
		t.Fatal(err)
	}
	assertDelta(true)
	if err := r.acknowledge(1, 2, false); err != nil || r.disabled {
		t.Fatal("old-revision feedback changed dictionary availability")
	}
	if err := r.acknowledge(1, 3, false); err != nil {
		t.Fatal(err)
	}
	assertDelta(false)
	if err := r.acknowledge(1, 3, true); err != nil {
		t.Fatal(err)
	}
	assertDelta(false)
	if _, err := r.installFull(v); err != nil {
		t.Fatal(err)
	}
	if err := r.acknowledge(1, ^uint64(0), false); err != nil || r.disabled || r.confirmedEpoch != 0 {
		t.Fatal("old-epoch feedback changed new dictionary")
	}
	if err := r.acknowledge(2, 3, true); err != nil {
		t.Fatal(err)
	}
	assertDelta(true)
}

func TestReplicationLifecycleKeepsReliableBirthState(t *testing.T) {
	var r replication
	_, err := r.installFull(View{Revision: 1})
	if err != nil {
		t.Fatal(err)
	}
	birth := Entity{ID: 2, Generation: 1, Dynamic: true, State: []byte{1, 2, 3}}
	update := cloneEntity(birth)
	update.State[0] = 9
	r.installChanges([]Change{{Entity: birth}, {Entity: update}}, 2)
	if !bytes.Equal(r.bases[2].state, birth.State) || r.bases[2].revision != 2 {
		t.Fatal("same-generation reliable update moved birth baseline")
	}
	r.installChanges([]Change{{Entity: update}}, 3)
	if !bytes.Equal(r.bases[2].state, birth.State) || r.bases[2].revision != 2 {
		t.Fatal("later reliable update moved birth baseline")
	}
	replacement := cloneEntity(update)
	replacement.Generation++
	r.installChanges([]Change{{Delete: true, Entity: update}, {Entity: replacement}}, 4)
	if r.bases[2].generation != 2 || r.bases[2].revision != 4 || !bytes.Equal(r.bases[2].state, replacement.State) {
		t.Fatal("replacement did not establish its reliable baseline")
	}
	r.installChanges([]Change{{Delete: true, Entity: replacement}}, 5)
	if len(r.bases) != 0 || r.baselineBytes() != 0 {
		t.Fatal("deleted entity retained baseline state")
	}
}

func TestReplicationBaselineBoundsCommitAtomically(t *testing.T) {
	es := make([]Entity, MaxEntities)
	for i := range es {
		es[i] = Entity{ID: uint32(i + 1), Generation: 1, State: make([]byte, MaxState)}
	}
	var r replication
	if _, err := r.installFull(View{Revision: 1, Entities: es}); err != nil {
		t.Fatal(err)
	}
	if len(r.bases) != MaxEntities || r.baselineBytes() != MaxEntities*MaxState {
		t.Fatal("full dictionary did not remain within its resource bound")
	}
	for _, v := range []View{
		{Entities: append(append([]Entity(nil), es...), Entity{ID: MaxEntities + 1, Generation: 1})},
		{Entities: []Entity{es[0], es[0]}},
		{Entities: []Entity{{ID: 1, Generation: 1, State: make([]byte, MaxState+1)}}},
		{Entities: []Entity{{ID: 0, Generation: 1}}},
		{ServerTime: -1},
	} {
		if _, err := r.installFull(v); !errors.Is(err, ErrProtocol) || r.epoch != 1 || r.revision != 1 || len(r.bases) != MaxEntities {
			t.Fatal("invalid full dictionary changed committed state")
		}
	}
	birth := Entity{ID: MaxEntities + 1, Generation: 1, State: make([]byte, MaxState)}
	if err := r.installChanges([]Change{{Entity: birth}}, 2); !errors.Is(err, ErrProtocol) || len(r.bases) != MaxEntities || r.revision != 1 {
		t.Fatal("over-capacity dictionary change was committed")
	}
	if err := r.installChanges([]Change{{Entity: birth}, {Delete: true, Entity: es[0]}}, 2); err != nil || len(r.bases) != MaxEntities || r.baselineBytes() != MaxEntities*MaxState {
		t.Fatalf("full-capacity create-before-delete rejected: %v", err)
	}
	if _, exists := r.bases[1]; exists || r.bases[MaxEntities+1].revision != 2 {
		t.Fatal("full-capacity replacement installed incorrect dictionary")
	}
	if err := r.installChanges([]Change{{Entity: Entity{ID: 3, Generation: 1, State: make([]byte, MaxState+1)}}}, 3); !errors.Is(err, ErrProtocol) || r.revision != 2 {
		t.Fatal("oversized dictionary record was committed")
	}
}

func TestSnapshotGroupsPreserveCoverageAndAck(t *testing.T) {
	es := make([]Entity, MaxEntities)
	for i := range es {
		es[i] = Entity{ID: uint32(MaxEntities - i), Generation: 1, Owner: 7, Dynamic: true, Ack: uint64(i * 100), State: make([]byte, 32)}
	}
	v := View{Tick: 3, Revision: 2, ServerTime: time.Second, Entities: es}
	var r replication
	if _, err := r.installFull(v); err != nil {
		t.Fatal(err)
	}
	if err := r.acknowledge(1, 2, true); err != nil {
		t.Fatal(err)
	}
	for i := range es {
		es[i].State = append([]byte(nil), es[i].State...)
		es[i].State[0] = byte(i)
	}
	groups, err := snapshotGroups(es, 600)
	if err != nil {
		t.Fatal(err)
	}
	full, _, err := r.build(v, groups, FullSnapshots)
	if err != nil {
		t.Fatal(err)
	}
	delta, _, err := r.build(v, groups, DeltaSnapshots)
	if err != nil || len(delta) != len(full) {
		t.Fatal("modes changed packet groups")
	}
	for i := range full {
		fp, ferr := decodePacket(full[i])
		dp, derr := decodePacket(delta[i])
		if ferr != nil || derr != nil || len(full[i]) > 600 || len(delta[i]) > 600 || len(fp.records) != len(dp.records) {
			t.Fatalf("packet %d violated budget/coverage", i)
		}
		for j, f := range fp.records {
			d := dp.records[j]
			if f.entity.ID != d.entity.ID || f.entity.Generation != d.entity.Generation || f.entity.Ack != d.entity.Ack {
				t.Fatal("mode changed entity metadata")
			}
			state := d.entity.State
			if d.delta {
				state, err = restoreDelta(state, r.bases[d.entity.ID].state)
			}
			if err != nil || !bytes.Equal(f.entity.State, state) {
				t.Fatal("delta reconstruction differs from full record")
			}
		}
	}
	for _, size := range []int{0, MaxState} {
		e := Entity{ID: ^uint32(0), Generation: ^uint32(0), Ack: ^uint64(0), Dynamic: true, State: make([]byte, size)}
		packets, err := snapshotPackets(1, 1, []Entity{e}, 600)
		if err != nil || len(packets) != 1 || len(packets[0]) > 600 {
			t.Fatalf("size %d failed 600-byte budget: %v", size, err)
		}
	}
}

func TestIndependentDeltaDatagrams(t *testing.T) {
	base := baseline{generation: 1, state: make([]byte, 32)}
	states := [][]byte{make([]byte, 32), make([]byte, 32)}
	states[0][0], states[1][1] = 7, 9
	packets := make([][]byte, 2)
	for i, state := range states {
		b := packetHeader(View{Tick: uint64(i + 1), ServerTime: time.Duration(i + 1)}, 1)
		record, delta := recordBytes(Entity{ID: 1, Generation: 1, Ack: uint64(i), State: state}, base, true)
		if !delta {
			t.Fatal("sparse state did not compress")
		}
		b = append(b, record...)
		le.PutUint16(b[34:36], 1)
		packets[i] = b
	}
	for _, i := range []int{1, 0, 1} {
		p, err := decodePacket(packets[i])
		if err != nil {
			t.Fatal(err)
		}
		got, err := restoreDelta(p.records[0].entity.State, base.state)
		if err != nil || !bytes.Equal(got, states[i]) {
			t.Fatal("reordered delta depended on previous packet")
		}
	}
}

func TestCompactPacketValidation(t *testing.T) {
	b := packetHeader(View{Tick: 1, ServerTime: time.Second}, 1)
	record, _ := recordBytes(Entity{ID: 1, Generation: 1, Ack: 4, State: []byte{7}}, baseline{}, false)
	b = append(b, record...)
	le.PutUint16(b[34:36], 1)
	for _, mutate := range []func([]byte) []byte{
		func(b []byte) []byte { return b[:35] },
		func(b []byte) []byte { le.PutUint64(b[26:34], 0); return b },
		func(b []byte) []byte { le.PutUint64(b[18:26], uint64(1)<<63); return b },
		func(b []byte) []byte { le.PutUint16(b[34:36], 0); return b },
		func(b []byte) []byte { b[36] = 0; return b },
		func(b []byte) []byte { le.PutUint16(b[34:36], 2); return append(b, record...) },
		func(b []byte) []byte { return append(b, 0) },
		func(b []byte) []byte { return append(append(b[:36], 0x81, 0), b[37:]...) },
	} {
		bad := mutate(append([]byte(nil), b...))
		if _, err := decodePacket(bad); !errors.Is(err, ErrProtocol) {
			t.Fatalf("invalid compact packet accepted: %x", bad)
		}
	}
}

func TestBaselineAckCodec(t *testing.T) {
	for _, available := range []bool{false, true} {
		b := encodeBaselineAck(2, 5, available)
		epoch, revision, got, err := decodeBaselineAck(b)
		if len(b) != 19 || epoch != 2 || revision != 5 || got != available || err != nil {
			t.Fatal("baseline confirmation round trip failed")
		}
	}
	for _, b := range [][]byte{encodeBaselineAck(0, 0, true), encodeBaselineAck(1, 2, true)[:18], append(encodeBaselineAck(1, 2, true), 0)} {
		if _, _, _, err := decodeBaselineAck(b); !errors.Is(err, ErrProtocol) {
			t.Fatal("invalid baseline confirmation accepted")
		}
	}
	b := encodeBaselineAck(1, 2, true)
	b[18] = 2
	if _, _, _, err := decodeBaselineAck(b); !errors.Is(err, ErrProtocol) {
		t.Fatal("invalid availability flag accepted")
	}
}

func TestV3ReliableHeaders(t *testing.T) {
	v := View{Tick: 7, Revision: 4, Player: 2, ServerTime: time.Second, Entities: []Entity{{ID: 1, Generation: 1, Owner: 2, Dynamic: true, State: []byte{9}}}}
	session := sessionState{epoch: 3}
	b := encodeFull(v, DefaultConfig(), session)
	got, cfg, gotSession, err := decodeFull(b)
	if err != nil || !reflect.DeepEqual(got, v) || cfg.SnapshotEncoding != DeltaSnapshots || gotSession.epoch != 3 || binary.LittleEndian.Uint16(b[91:93]) != 1 {
		t.Fatalf("complete header failed: %v", err)
	}
	changes := []Change{{Entity: v.Entities[0]}}
	b = encodeChanges(v.Tick, v.Revision, v.ServerTime, changes)
	tick, revision, at, gotChanges, err := decodeChanges(b)
	if err != nil || tick != v.Tick || revision != v.Revision || at != v.ServerTime || !reflect.DeepEqual(gotChanges, changes) || binary.LittleEndian.Uint16(b[26:28]) != 1 {
		t.Fatalf("change header failed: %v", err)
	}
}

func FuzzDelta(f *testing.F) {
	f.Add(make([]byte, 32), append([]byte{7}, make([]byte, 31)...))
	f.Add([]byte{1}, []byte{2})
	f.Fuzz(func(t *testing.T, base, state []byte) {
		if len(base) > MaxState || len(state) > MaxState {
			return
		}
		payload, delta := stateDelta(state, base)
		if !delta {
			if !bytes.Equal(payload, state) {
				t.Fatal("fallback changed state")
			}
			return
		}
		got, err := restoreDelta(payload, base)
		if err != nil || !bytes.Equal(got, state) || len(payload) >= len(state) {
			t.Fatal("invalid difference encoding")
		}
	})
}
