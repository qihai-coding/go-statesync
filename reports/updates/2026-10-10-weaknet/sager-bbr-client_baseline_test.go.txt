package statesync

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/sagernet/quic-go"
)

func TestClientFullBaselinesOwnOnlyTheirState(t *testing.T) {
	for _, size := range []int{1, 33, MaxState} {
		v := View{Player: 1, Revision: 1, Entities: []Entity{
			{ID: 1, Generation: 1, Owner: 1, Dynamic: true, State: bytes.Repeat([]byte{3}, size)},
			{ID: 2, Generation: 1, Owner: 2, Dynamic: true, State: bytes.Repeat([]byte{7}, size)},
		}}
		packet := encodeFull(v, DefaultConfig(), sessionState{epoch: 1})
		c := &Client{model: vectorModel{}}
		if err := c.applyFull(packet); err != nil {
			t.Fatal(err)
		}
		clear(packet)
		for _, expected := range v.Entities {
			tracked := c.entities[expected.ID]
			base := c.bases[expected.ID].state
			if !bytes.Equal(base, expected.State) || !bytes.Equal(tracked.entity.State, expected.State) || cap(base) != size ||
				&base[0] != &tracked.entity.State[0] || &base[0] != &tracked.samples[0].state[0] {
				t.Fatalf("size %d: full state retained its frame or duplicated baseline storage", size)
			}
		}
	}
}

func TestClientLifecycleBaselinesDetachFramesOnce(t *testing.T) {
	c, _ := vectorClient(t)
	e := Entity{ID: 3, Generation: 1, Owner: 3, Dynamic: true, State: bytes.Repeat([]byte{7}, 33)}
	for tick := uint64(1); tick <= 2; tick++ {
		if tick == 2 {
			e.Generation++
			e.State = bytes.Repeat([]byte{9}, 33)
		}
		padding := c.entities[2].entity
		padding.State = make([]byte, MaxState)
		packet := encodeChanges(tick, tick+1, time.Duration(tick)*time.Second/30, []Change{{Entity: e}, {Entity: padding}})
		if err := c.applyChanges(packet); err != nil {
			t.Fatal(err)
		}
		clear(packet)
		tracked := c.entities[e.ID]
		base := c.bases[e.ID].state
		if !bytes.Equal(base, e.State) || !bytes.Equal(tracked.entity.State, e.State) || cap(base) != len(e.State) ||
			&base[0] != &tracked.entity.State[0] || &base[0] != &tracked.samples[0].state[0] {
			t.Fatal("creation or replacement retained its frame or duplicated baseline storage")
		}
	}
	base := c.bases[e.ID].state
	e.State = bytes.Repeat([]byte{12}, 33)
	if err := c.applyChanges(encodeChanges(3, 4, 100*time.Millisecond, []Change{{Entity: e}})); err != nil {
		t.Fatal(err)
	}
	if &base[0] != &c.bases[e.ID].state[0] || base[0] != 9 || c.entities[e.ID].entity.State[0] != 12 {
		t.Fatal("same-generation update moved the immutable baseline")
	}
}

func TestClientMissingDictionaryFeedbackAndFullRecovery(t *testing.T) {
	cert, pem, err := LocalCertificate()
	if err != nil {
		t.Fatal(err)
	}
	tls, err := ClientTLS(pem, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", ServerTLS(cert), transportConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, listener.Addr().String(), tls, transportConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(closeLeave, "test end")
	server, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer server.CloseWithError(closeLeave, "test end")
	c, r := vectorClient(t)
	c.conn = conn
	c.reliable = make(chan []byte, 4)
	delete(c.bases, 2)
	e := cloneEntity(c.entities[2].entity)
	e.State[0] = 7
	c.wg.Add(1)
	go c.readDatagrams()
	defer func() { conn.CloseWithError(closeLeave, "test end"); c.wg.Wait() }()
	if err := server.SendDatagram(clientDeltaPacket(t, &r, 1, e)); err != nil {
		t.Fatal(err)
	}
	var feedback []byte
	select {
	case feedback = <-c.reliable:
	case <-ctx.Done():
		t.Fatal("missing dictionary did not emit feedback")
	}
	epoch, revision, available, err := decodeBaselineAck(feedback)
	if err != nil || epoch != 1 || revision != 1 || available {
		t.Fatal("missing dictionary feedback was not unavailable", err)
	}
	c.mu.Lock()
	broken, value, tick := c.baselineBroken, c.entities[2].entity.State[0], c.tick
	c.mu.Unlock()
	if !broken || value != 0 || tick != 0 {
		t.Fatal("unrestorable delta changed state or clock")
	}
	if err := r.acknowledge(epoch, revision, available); err != nil {
		t.Fatal(err)
	}
	groups, err := snapshotGroups([]Entity{e}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	packets, stats, err := r.build(View{Tick: 2, Revision: 1, ServerTime: 2 * time.Second / 30}, groups, DeltaSnapshots)
	if err != nil || stats.fullRecords != 1 || len(packets) != 1 {
		t.Fatal("unavailable dictionary did not downgrade to full records", err)
	}
	if err := server.SendDatagram(packets[0]); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		c.mu.Lock()
		applied := c.entities[2].entity.State[0] == 7
		c.mu.Unlock()
		if applied {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("full fallback did not apply")
		}
	}
	v := c.Authoritative()
	epoch, err = r.installFull(v)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	if !c.baselineBroken || c.bases[2].state != nil {
		c.mu.Unlock()
		t.Fatal("full fallback silently repaired the immutable dictionary")
	}
	c.resyncing = true
	err = c.applyFull(encodeFull(v, DefaultConfig(), sessionState{epoch: epoch}))
	repaired := !c.baselineBroken && c.epoch == 2 && bytes.Equal(c.bases[2].state, e.State)
	c.mu.Unlock()
	if err != nil || !repaired {
		t.Fatal("full resync did not repair the dictionary", err)
	}
	if err := r.acknowledge(epoch, v.Revision, true); err != nil {
		t.Fatal(err)
	}
	e.State[0] = 9
	packet := clientDeltaPacket(t, &r, 3, e)
	c.mu.Lock()
	err = c.applySnapshot(packet)
	value = c.entities[2].entity.State[0]
	c.mu.Unlock()
	if err != nil || value != 9 {
		t.Fatal("repaired dictionary did not accept a delta", err)
	}
}
