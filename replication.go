package statesync

import (
	"math/bits"
	"sort"
	"time"
)

type baseline struct {
	generation uint32
	revision   uint64
	state      []byte
}

// Every delta references the immutable reliable state, never a prior datagram.
func stateDelta(state, base []byte) ([]byte, bool) {
	if len(state) != len(base) || len(state) == 0 {
		return state, false
	}
	n := (len(state) + 7) / 8
	changed := 0
	for i, v := range state {
		if v != base[i] {
			changed++
		}
	}
	if n+changed >= len(state) {
		return state, false
	}
	b := make([]byte, n, n+changed)
	for i, v := range state {
		if v != base[i] {
			b[i/8] |= 1 << uint(i%8)
			b = append(b, v)
		}
	}
	return b, true
}

func restoreDelta(b, base []byte) ([]byte, error) {
	n := (len(base) + 7) / 8
	if n == 0 || len(base) > MaxState || len(b) < n || len(b) >= len(base) {
		return nil, ErrProtocol
	}
	if len(base)%8 != 0 && b[n-1]>>uint(len(base)%8) != 0 {
		return nil, ErrProtocol
	}
	out := append([]byte(nil), base...)
	j := n
	for i := range out {
		if b[i/8]&(1<<uint(i%8)) != 0 {
			if j >= len(b) || b[j] == out[i] {
				return nil, ErrProtocol
			}
			out[i] = b[j]
			j++
		}
	}
	if j != len(b) {
		return nil, ErrProtocol
	}
	return out, nil
}

type snapshotRecord struct {
	entity Entity
	delta  bool
}

type snapshotPacket struct {
	tick, revision, epoch uint64
	at                    time.Duration
	records               []snapshotRecord
}

func packetHeader(v View, epoch uint64) encoder {
	b := stateHeader(msgSnapshot, v.Tick, v.Revision)
	b.u64(uint64(v.ServerTime))
	b.u64(epoch)
	b.u16(0)
	return b
}

func recordBytes(e Entity, base baseline, compress bool) (encoder, bool) {
	state, delta := e.State, false
	if compress && e.Generation == base.generation {
		state, delta = stateDelta(state, base.state)
	}
	b := encoder(make([]byte, 0, 23+len(state)))
	b.var32(e.ID)
	b.var32(e.Generation)
	b.var64(e.Ack)
	field := uint32(len(state)) << 1
	if delta {
		field |= 1
	}
	b.var32(field)
	b = append(b, state...)
	return b, delta
}

func decodePacket(b []byte) (snapshotPacket, error) {
	var p snapshotPacket
	if len(b) > 1000 {
		return p, ErrProtocol
	}
	d := decode(b, msgSnapshot)
	p.tick, p.revision = d.u64(), d.u64()
	p.at, p.epoch = time.Duration(d.u64()), d.u64()
	n := int(d.u16())
	if p.at < 0 || p.epoch == 0 || n < 1 || n > MaxEntities {
		return p, ErrProtocol
	}
	p.records = make([]snapshotRecord, n)
	seen := make(map[uint32]bool, n)
	for i := range p.records {
		e := Entity{ID: d.var32(), Generation: d.var32(), Ack: d.var64(), Dynamic: true}
		field := d.var32()
		size := int(field >> 1)
		if size > MaxState {
			return p, ErrProtocol
		}
		e.State = d.take(size)
		if !validEntity(e) || seen[e.ID] {
			return p, ErrProtocol
		}
		seen[e.ID] = true
		p.records[i] = snapshotRecord{entity: e, delta: field&1 != 0}
	}
	return p, d.end()
}

func varSize(v uint64) int { return max(1, (bits.Len64(v)+6)/7) }

// Group by full-record size so compression cannot change frame coverage.
func snapshotGroups(es []Entity, limit int) ([][]Entity, error) {
	if len(es) > MaxEntities || limit < 36 || limit > 1000 {
		return nil, ErrProtocol
	}
	dynamic := make([]Entity, 0, len(es))
	for _, e := range es {
		if e.Dynamic {
			if !validEntity(e) {
				return nil, ErrProtocol
			}
			dynamic = append(dynamic, e)
		}
	}
	sort.Slice(dynamic, func(i, j int) bool { return dynamic[i].ID < dynamic[j].ID })
	var groups [][]Entity
	start, size := 0, 36
	for i, e := range dynamic {
		if i > 0 && dynamic[i-1].ID == e.ID {
			return nil, ErrProtocol
		}
		recordSize := varSize(uint64(e.ID)) + varSize(uint64(e.Generation)) + varSize(e.Ack) + varSize(uint64(len(e.State))<<1) + len(e.State)
		if recordSize+36 > limit {
			return nil, ErrProtocol
		}
		if size+recordSize > limit {
			groups = append(groups, dynamic[start:i])
			start, size = i, 36
		}
		size += recordSize
	}
	if start < len(dynamic) {
		groups = append(groups, dynamic[start:])
	}
	return groups, nil
}

type recordStats struct {
	deltaBytes, fullBytes, deltaRecords, fullRecords uint64
}

// The room owner exclusively manages connection baselines and confirmations.
type replication struct {
	epoch, revision, confirmedEpoch, confirmedRevision uint64
	bases                                              map[uint32]baseline
	disabled                                           bool
}

func (r *replication) installFull(v View) (uint64, error) {
	if r.epoch == ^uint64(0) || len(v.Entities) > MaxEntities || v.ServerTime < 0 {
		return 0, ErrProtocol
	}
	bases := make(map[uint32]baseline, len(v.Entities))
	for _, e := range v.Entities {
		if !validEntity(e) {
			return 0, ErrProtocol
		}
		if _, exists := bases[e.ID]; exists {
			return 0, ErrProtocol
		}
		bases[e.ID] = baseline{generation: e.Generation, revision: v.Revision, state: e.State}
	}
	r.epoch++
	r.revision = v.Revision
	r.confirmedEpoch, r.confirmedRevision = 0, 0
	r.disabled = false
	r.bases = bases
	return r.epoch, nil
}

func (r *replication) installChanges(changes []Change, revision uint64) error {
	if len(changes) > MaxEntities*2 {
		return ErrProtocol
	}
	type stagedBaseline struct {
		base   baseline
		exists bool
	}
	staged := make(map[uint32]stagedBaseline, len(changes))
	for _, change := range changes {
		e := change.Entity
		if !validEntity(e) {
			return ErrProtocol
		}
		next, touched := staged[e.ID]
		if !touched {
			next.base, next.exists = r.bases[e.ID]
		}
		if change.Delete {
			next.exists = false
		} else if !next.exists || next.base.generation != e.Generation {
			next = stagedBaseline{base: baseline{generation: e.Generation, revision: revision, state: e.State}, exists: true}
		}
		staged[e.ID] = next
	}
	count := len(r.bases)
	for id, next := range staged {
		_, existed := r.bases[id]
		if existed && !next.exists {
			count--
		} else if !existed && next.exists {
			count++
		}
	}
	if count > MaxEntities {
		return ErrProtocol
	}
	for id, next := range staged {
		if old, exists := r.bases[id]; exists && (!next.exists || old.generation != next.base.generation) {
			delete(r.bases, id)
		}
	}
	if r.bases == nil {
		r.bases = make(map[uint32]baseline, count)
	}
	for id, next := range staged {
		if next.exists {
			r.bases[id] = next.base
		}
	}
	r.revision = revision
	return nil
}

func (r *replication) acknowledge(epoch, revision uint64, available bool) error {
	if epoch > r.epoch || epoch == r.epoch && revision > r.revision {
		return ErrProtocol
	}
	if epoch < r.epoch || revision < r.confirmedRevision {
		return nil
	}
	r.confirmedEpoch, r.confirmedRevision = epoch, revision
	r.disabled = r.disabled || !available
	return nil
}

func (r *replication) baselineBytes() int {
	n := 0
	for _, b := range r.bases {
		n += len(b.state)
	}
	return n
}

func (r *replication) build(v View, groups [][]Entity, mode SnapshotEncoding) ([][]byte, recordStats, error) {
	var stats recordStats
	if r.epoch == 0 || v.Revision != r.revision || v.ServerTime < 0 {
		return nil, stats, ErrProtocol
	}
	packets := make([][]byte, 0, len(groups))
	for _, group := range groups {
		b := packetHeader(v, r.epoch)
		for _, e := range group {
			base := r.bases[e.ID]
			eligible := mode == DeltaSnapshots && !r.disabled && r.confirmedEpoch == r.epoch && base.revision <= r.confirmedRevision
			record, delta := recordBytes(e, base, eligible)
			b = append(b, record...)
			if delta {
				stats.deltaBytes += uint64(len(record))
				stats.deltaRecords++
			} else {
				stats.fullBytes += uint64(len(record))
				stats.fullRecords++
			}
		}
		le.PutUint16(b[34:36], uint16(len(group)))
		packets = append(packets, b)
	}
	return packets, stats, nil
}

// Full encoding helpers also serve protocol tests and benchmarks.
func snapshotPackets(tick, revision uint64, es []Entity, limit int) ([][]byte, error) {
	groups, err := snapshotGroups(es, limit)
	if err != nil {
		return nil, err
	}
	v := View{Tick: tick, Revision: revision, ServerTime: time.Duration(tick) * time.Second / 30}
	r := replication{epoch: 1, revision: revision}
	packets, _, err := r.build(v, groups, FullSnapshots)
	return packets, err
}

func decodeSnapshot(b []byte) (uint64, uint64, []Entity, error) {
	p, err := decodePacket(b)
	if err != nil {
		return 0, 0, nil, err
	}
	es := make([]Entity, len(p.records))
	for i, record := range p.records {
		if record.delta {
			return 0, 0, nil, ErrProtocol
		}
		es[i] = record.entity
	}
	return p.tick, p.revision, es, nil
}
