package statesync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/quic-go/qlog"
)

func TestDatagramTraceIsServerOnlyAndConnectionOwned(t *testing.T) {
	if transportConfig().Tracer != nil {
		t.Fatal("client transport unexpectedly enables tracing")
	}
	cfg := serverTransportConfig()
	a := cfg.Tracer(context.Background(), false, quic.ConnectionID{}).(*datagramSendTrace)
	b := cfg.Tracer(context.Background(), false, quic.ConnectionID{}).(*datagramSendTrace)
	if a == b || a.sent == b.sent || cap(a.sent) != 1 || cap(b.sent) != 1 {
		t.Fatal("connections share notifications or have an unbounded queue")
	}
	if a.AddProducer() != a || !a.SupportsSchemas(qlog.EventSchema) || a.SupportsSchemas("other") || a.Close() != nil {
		t.Fatal("invalid trace lifecycle")
	}
}

func TestDatagramTraceSignalsOnlyDequeuedDatagrams(t *testing.T) {
	trace := &datagramSendTrace{sent: make(chan struct{}, 1)}
	trace.RecordEvent(qlog.PacketReceived{})
	trace.RecordEvent(qlog.PacketSent{Frames: []qlog.Frame{{Frame: &qlog.StreamFrame{StreamID: 1}}}})
	if len(trace.sent) != 0 {
		t.Fatal("non-DATAGRAM event released the sender")
	}
	packet := qlog.PacketSent{Frames: []qlog.Frame{{Frame: &qlog.StreamFrame{StreamID: 1}}, {Frame: &qlog.DatagramFrame{Length: 200}}}}
	for range 10 {
		trace.RecordEvent(packet)
	}
	if len(trace.sent) != 1 {
		t.Fatal("send notification queue is unbounded")
	}
	// Notification may arrive before SendDatagram returns; it must remain observable.
	if err := trace.wait(context.Background()); err != nil || len(trace.sent) != 0 {
		t.Fatal("early send notification was lost")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- trace.wait(ctx) }()
	select {
	case <-done:
		t.Fatal("sender advanced without a DATAGRAM notification")
	default:
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("sender returned incorrect cancellation error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("connection cancellation did not release sender")
	}
}
