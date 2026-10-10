package statesync_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/arena"
	officialquic "github.com/quic-go/quic-go"
)

func writeInteropFrame(t *testing.T, w io.Writer, body []byte) {
	t.Helper()
	frame := binary.LittleEndian.AppendUint32(nil, uint32(len(body)))
	if _, err := io.Copy(w, bytes.NewReader(append(frame, body...))); err != nil {
		t.Fatal(err)
	}
}

func readInteropFrame(t *testing.T, r io.Reader) []byte {
	t.Helper()
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		t.Fatal(err)
	}
	n := binary.LittleEndian.Uint32(header[:])
	if n < 2 || n > syncnet.MaxFrame {
		t.Fatal("invalid reliable frame length", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		t.Fatal(err)
	}
	if body[0] != syncnet.ProtocolVersion {
		t.Fatal("transport migration changed application protocol", body[0])
	}
	return body
}

func TestOfficialQUICClientInteroperatesWithBBRServer(t *testing.T) {
	f := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	conn, err := officialquic.DialAddr(ctx, f.s.Addr().String(), f.tls, &officialquic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(10, "test end")
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deadline, _ := ctx.Deadline()
	stream.SetReadDeadline(deadline)
	join := append([]byte{syncnet.ProtocolVersion, 1, 5, 0, 'a', 'l', 'p', 'h', 'a'}, make([]byte, 32)...)
	writeInteropFrame(t, stream, join)
	full := readInteropFrame(t, stream)
	if len(full) != 93+17*36 || full[1] != 2 || full[42] != byte(syncnet.DeltaSnapshots) || binary.LittleEndian.Uint16(full[91:93]) != 17 {
		t.Fatal("initial full state no longer follows version three layout")
	}
	player := binary.LittleEndian.Uint32(full[34:38])
	epoch, revision := binary.LittleEndian.Uint64(full[26:34]), binary.LittleEndian.Uint64(full[10:18])
	initial := arena.Encode(arena.State{Kind: arena.Player})
	controlled := full[93+16*36:]
	if player == 0 || epoch == 0 || binary.LittleEndian.Uint32(controlled[:4]) != 17 ||
		binary.LittleEndian.Uint32(controlled[8:12]) != player || controlled[12] != 1 || !bytes.Equal(controlled[23:], initial) {
		t.Fatal("official client did not receive its controlled entity")
	}
	confirm := func(rev uint64) {
		ack := binary.LittleEndian.AppendUint64([]byte{syncnet.ProtocolVersion, 10}, epoch)
		ack = binary.LittleEndian.AppendUint64(ack, rev)
		writeInteropFrame(t, stream, append(ack, 1))
	}
	confirm(revision)
	input := binary.LittleEndian.AppendUint64([]byte{syncnet.ProtocolVersion, 7, 1}, 1)
	input = binary.LittleEndian.AppendUint16(input, 8)
	input = append(input, arena.Move(1, 0)...)
	if err := conn.SendDatagram(input); err != nil {
		t.Fatal(err)
	}
	predicted, err := (arena.Model{}).Predict(initial, arena.Move(1, 0), 1.0/30)
	if err != nil {
		t.Fatal(err)
	}
	// The known 13-byte state changes only X, so the exact independent bitmap is fixed.
	wantRecord := append([]byte{17, 1, 1, 13, 0x1e, 0}, predicted[1:5]...)
	for {
		packet, err := conn.ReceiveDatagram(ctx)
		if err != nil {
			t.Fatal("official client did not receive an acknowledged delta", err)
		}
		if len(packet) < 36 || packet[0] != syncnet.ProtocolVersion || packet[1] != 8 {
			t.Fatal("invalid version three datagram")
		}
		if binary.LittleEndian.Uint64(packet[10:18]) == revision && binary.LittleEndian.Uint64(packet[26:34]) == epoch &&
			binary.LittleEndian.Uint16(packet[34:36]) == 1 && bytes.Equal(packet[36:], wantRecord) {
			break
		}
	}
	v, metrics := inspect(t, f.r)
	if metrics.DeltaRecords == 0 || len(v.Entities) != 17 {
		t.Fatal("reliable baseline confirmation did not enable deltas")
	}
	for _, e := range v.Entities {
		if e.Owner == player && (e.Ack != 1 || !bytes.Equal(e.State, predicted)) {
			t.Fatal("official input did not advance authoritative state")
		}
	}
	observer := connect(t, f, f.s.Addr().String(), "alpha")
	for _, deleted := range []bool{false, true} {
		if deleted {
			observer.Close()
		}
		change := readInteropFrame(t, stream)
		if len(change) != 28+1+36 || change[1] != 3 || binary.LittleEndian.Uint16(change[26:28]) != 1 ||
			(change[28] == 1) != deleted || binary.LittleEndian.Uint32(change[29:33]) != 18 {
			t.Fatal("official client did not receive ordered create/delete lifecycle")
		}
		revision++
		if binary.LittleEndian.Uint64(change[10:18]) != revision {
			t.Fatal("lifecycle revision changed during transport migration")
		}
		confirm(revision)
	}
	conn.CloseWithError(10, "explicit leave")
	eventually(t, 2*time.Second, func() bool {
		_, metrics := inspect(t, f.r)
		return metrics.Players == 0 && metrics.RetainedPlayers == 0
	})
}

func TestBBRStandardConcurrentConnectionsInputsAndActions(t *testing.T) {
	f := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	const count = 8
	clients := make([]*syncnet.Client, count)
	dialErrors := make(chan error, count)
	var workers sync.WaitGroup
	for i := range clients {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var err error
			clients[i], err = syncnet.Dial(ctx, f.s.Addr().String(), "alpha", f.tls, arena.Model{})
			dialErrors <- err
		}()
	}
	workers.Wait()
	for _, c := range clients {
		if c != nil {
			t.Cleanup(c.Close)
		}
	}
	for range count {
		if err := <-dialErrors; err != nil {
			t.Fatal("concurrent connection setup failed", err)
		}
	}
	results := make(chan bool, count)
	inputErrors := make(chan error, count)
	for _, c := range clients {
		workers.Add(1)
		go func() {
			defer workers.Done()
			inputsDone := make(chan error, 1)
			go func() {
				ticker := time.NewTicker(time.Second / 30)
				defer ticker.Stop()
				for range 12 {
					select {
					case <-ticker.C:
					case <-ctx.Done():
						inputsDone <- ctx.Err()
						return
					}
					if err := c.SubmitInput(arena.Move(0, 0)); err != nil {
						inputsDone <- err
						return
					}
				}
				inputsDone <- nil
			}()
			_, err := c.Action(ctx, 1, arena.Pickup(1, 1))
			results <- err == nil
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, syncnet.ErrClosed) || errors.Is(err, syncnet.ErrProtocol) {
				inputErrors <- err
				<-inputsDone
			} else {
				inputErrors <- <-inputsDone
			}
		}()
	}
	workers.Wait()
	wins := 0
	for range count {
		if err := <-inputErrors; err != nil {
			t.Fatal("input or action failed during concurrent transport activity", err)
		}
		if <-results {
			wins++
		}
	}
	if wins != 1 {
		t.Fatal("concurrent action was not applied exactly once", wins)
	}
	eventually(t, 2*time.Second, func() bool {
		for _, c := range clients {
			if c.Err() != nil || own(c).Ack != 12 || len(c.Authoritative().Entities) != 16+count-1 {
				return false
			}
		}
		return true
	})
	for _, c := range clients {
		c.Close()
	}
	eventually(t, 2*time.Second, func() bool {
		_, metrics := inspect(t, f.r)
		return metrics.Players == 0 && metrics.RetainedPlayers == 0
	})
}
