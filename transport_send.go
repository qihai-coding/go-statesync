package statesync

import (
	"context"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/quic-go/qlog"
	"github.com/sagernet/quic-go/qlogwriter"
)

func serverTransportConfig() *quic.Config {
	cfg := transportConfig()
	cfg.Tracer = func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace {
		return &datagramSendTrace{sent: make(chan struct{}, 1)}
	}
	return cfg
}

type datagramSendTrace struct{ sent chan struct{} }

func (t *datagramSendTrace) AddProducer() qlogwriter.Recorder { return t }
func (*datagramSendTrace) SupportsSchemas(schema string) bool { return schema == qlog.EventSchema }
func (*datagramSendTrace) Close() error                       { return nil }

func (t *datagramSendTrace) RecordEvent(event qlogwriter.Event) {
	p, ok := event.(qlog.PacketSent)
	if !ok {
		return
	}
	for _, frame := range p.Frames {
		if _, ok := frame.Frame.(*qlog.DatagramFrame); ok {
			// PacketSent follows DATAGRAM dequeue, before the UDP write queue.
			select {
			case t.sent <- struct{}{}:
			default:
			}
			return
		}
	}
}

func (t *datagramSendTrace) wait(ctx context.Context) error {
	select {
	case <-t.sent:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
