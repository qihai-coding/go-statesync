package statesync

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	msgJoin     byte = 1
	msgFull     byte = 2
	msgChanges  byte = 3
	msgAction   byte = 4
	msgResult   byte = 5
	msgResync   byte = 6
	msgInputs   byte = 7
	msgSnapshot byte = 8
	msgResume   byte = 9

	// Sizes include version and type, but exclude the stream length prefix.
	maxJoinFrame   = 2 + 2 + 64 + 32
	maxClientFrame = 2 + 8 + 2 + MaxAction
)

type sessionState struct {
	token      ResumeToken
	lastAction uint64
}

var le = binary.LittleEndian

type encoder []byte

func start(kind byte) encoder     { return encoder{ProtocolVersion, kind} }
func (e *encoder) u8(v byte)      { *e = append(*e, v) }
func (e *encoder) u16(v uint16)   { *e = le.AppendUint16(*e, v) }
func (e *encoder) u32(v uint32)   { *e = le.AppendUint32(*e, v) }
func (e *encoder) u64(v uint64)   { *e = le.AppendUint64(*e, v) }
func (e *encoder) bytes(b []byte) { e.u16(uint16(len(b))); *e = append(*e, b...) }
func (e *encoder) entity(v Entity) {
	e.u32(v.ID)
	e.u32(v.Generation)
	e.u32(v.Owner)
	if v.Dynamic {
		e.u8(1)
	} else {
		e.u8(0)
	}
	e.u64(v.Ack)
	e.bytes(v.State)
}

type decoder struct {
	b   []byte
	err error
}

func decode(b []byte, kind byte) *decoder {
	d := &decoder{b: b}
	if d.u8() != ProtocolVersion || d.u8() != kind {
		d.err = ErrProtocol
	}
	return d
}
func (d *decoder) take(n int) []byte {
	if d.err != nil || n < 0 || len(d.b) < n {
		d.err = ErrProtocol
		return make([]byte, n)
	}
	b := d.b[:n]
	d.b = d.b[n:]
	return b
}
func (d *decoder) u8() byte    { return d.take(1)[0] }
func (d *decoder) u16() uint16 { return le.Uint16(d.take(2)) }
func (d *decoder) u32() uint32 { return le.Uint32(d.take(4)) }
func (d *decoder) u64() uint64 { return le.Uint64(d.take(8)) }
func (d *decoder) bytes(max int) []byte {
	n := int(d.u16())
	if n > max {
		d.err = ErrProtocol
		return nil
	}
	return d.take(n)
}
func (d *decoder) entity() Entity {
	e := Entity{ID: d.u32(), Generation: d.u32(), Owner: d.u32()}
	f := d.u8()
	if f > 1 {
		d.err = ErrProtocol
	}
	e.Dynamic = f == 1
	e.Ack = d.u64()
	e.State = d.bytes(MaxState)
	if !validEntity(e) {
		d.err = ErrProtocol
	}
	return e
}
func (d *decoder) end() error {
	if d.err != nil || len(d.b) != 0 {
		return ErrProtocol
	}
	return nil
}
func writeFrame(w io.Writer, b []byte) error {
	if len(b) < 2 || len(b) > MaxFrame {
		return ErrProtocol
	}
	var header [4]byte
	le.PutUint32(header[:], uint32(len(b)))
	// Complete short writes instead of silently truncating a frame.
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, b)
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func readFrame(r io.Reader, limit uint32) ([]byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	n := le.Uint32(h[:])
	if n < 2 || n > min(limit, MaxFrame) {
		return nil, ErrProtocol
	}
	b := make([]byte, int(n))
	_, err := io.ReadFull(r, b)
	if err == nil && b[0] != ProtocolVersion {
		err = ErrProtocol
	}
	return b, err
}
func encodeInputs(in []Input) []byte {
	b := start(msgInputs)
	b.u8(byte(len(in)))
	for _, v := range in {
		b.u64(v.Sequence)
		b.bytes(v.Data)
	}
	return b
}
func decodeInputs(b []byte) ([]Input, error) {
	if len(b) > 1000 {
		return nil, ErrProtocol
	}
	d := decode(b, msgInputs)
	n := int(d.u8())
	if n < 1 || n > 3 {
		return nil, ErrProtocol
	}
	values := make([]Input, n)
	for i := range values {
		values[i] = Input{Sequence: d.u64(), Data: d.bytes(MaxInput)}
		if values[i].Sequence == 0 || (i > 0 && values[i].Sequence <= values[i-1].Sequence) {
			return nil, ErrProtocol
		}
	}
	return values, d.end()
}
func stateHeader(kind byte, tick, rev uint64) encoder {
	b := start(kind)
	b.u64(tick)
	b.u64(rev)
	return b
}
func encodeFull(v View, cfg Config, session sessionState) []byte {
	b := stateHeader(msgFull, v.Tick, v.Revision)
	b.u32(v.Player)
	b.u16(uint16(cfg.TickRate))
	b.u16(uint16(cfg.SnapshotRate))
	b.u64(uint64(cfg.ResumeGracePeriod))
	b = append(b, session.token[:]...)
	b.u64(session.lastAction)
	b.u16(uint16(len(v.Entities)))
	for _, e := range v.Entities {
		b.entity(e)
	}
	return b
}
func decodeFull(b []byte) (View, Config, sessionState, error) {
	d := decode(b, msgFull)
	v := View{Tick: d.u64(), Revision: d.u64(), Player: d.u32()}
	c := DefaultConfig()
	c.TickRate = int(d.u16())
	c.SnapshotRate = int(d.u16())
	c.ResumeGracePeriod = time.Duration(d.u64())
	session := sessionState{}
	copy(session.token[:], d.take(32))
	session.lastAction = d.u64()
	n := int(d.u16())
	if n > MaxEntities || v.Player == 0 || c.validate() != nil {
		return v, c, session, ErrProtocol
	}
	v.Entities = make([]Entity, n)
	seen := make(map[uint32]bool, n)
	for i := range v.Entities {
		v.Entities[i] = d.entity()
		id := v.Entities[i].ID
		if seen[id] {
			return v, c, session, ErrProtocol
		}
		seen[id] = true
	}
	return v, c, session, d.end()
}
func encodeChanges(tick, rev uint64, changes []Change) []byte {
	b := stateHeader(msgChanges, tick, rev)
	b.u16(uint16(len(changes)))
	for _, v := range changes {
		if v.Delete {
			b.u8(1)
		} else {
			b.u8(0)
		}
		b.entity(v.Entity)
	}
	return b
}
func decodeChanges(b []byte) (uint64, uint64, []Change, error) {
	d := decode(b, msgChanges)
	t, r := d.u64(), d.u64()
	n := int(d.u16())
	if n > MaxEntities*2 {
		return 0, 0, nil, ErrProtocol
	}
	out := make([]Change, n)
	for i := range out {
		f := d.u8()
		if f > 1 {
			return 0, 0, nil, ErrProtocol
		}
		out[i] = Change{Delete: f == 1, Entity: d.entity()}
	}
	return t, r, out, d.end()
}
func snapshotPackets(tick, rev uint64, entities []Entity, limit int) ([][]byte, error) {
	var packets [][]byte
	b := stateHeader(msgSnapshot, tick, rev)
	b.u16(0)
	count := uint16(0)
	flush := func() {
		if count > 0 {
			le.PutUint16(b[18:20], count)
			packets = append(packets, b)
		}
		b = stateHeader(msgSnapshot, tick, rev)
		b.u16(0)
		count = 0
	}
	for _, e := range entities {
		if !e.Dynamic {
			continue
		}
		size := 23 + len(e.State)
		if !validEntity(e) || size+20 > limit {
			return nil, fmt.Errorf("entity %d exceeds datagram budget", e.ID)
		}
		if len(b)+size > limit {
			flush()
		}
		b.entity(e)
		count++
	}
	flush()
	return packets, nil
}
func decodeSnapshot(b []byte) (uint64, uint64, []Entity, error) {
	if len(b) > 1000 {
		return 0, 0, nil, ErrProtocol
	}
	d := decode(b, msgSnapshot)
	tick, rev := d.u64(), d.u64()
	n := int(d.u16())
	if n < 1 || n > MaxEntities {
		return 0, 0, nil, ErrProtocol
	}
	out := make([]Entity, n)
	for i := range out {
		out[i] = d.entity()
	}
	return tick, rev, out, d.end()
}
