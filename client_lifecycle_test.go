package statesync_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/arena"
	"github.com/quic-go/quic-go"
)

func TestDialCancellationWhileAwaitingFullState(t *testing.T) {
	for _, resume := range []bool{false, true} {
		for _, mode := range []string{"cancel", "deadline", "cause"} {
			t.Run(map[bool]string{false: "join", true: "resume"}[resume]+"/"+mode, func(t *testing.T) {
				cert, pem, err := syncnet.LocalCertificate()
				if err != nil {
					t.Fatal(err)
				}
				tc, err := syncnet.ClientTLS(pem, "localhost")
				if err != nil {
					t.Fatal(err)
				}
				ln, err := quic.ListenAddr("127.0.0.1:0", syncnet.ServerTLS(cert), &quic.Config{EnableDatagrams: true})
				if err != nil {
					t.Fatal(err)
				}
				defer ln.Close()
				deadline, stop := context.WithTimeout(context.Background(), 2*time.Second)
				defer stop()
				ctx, cancel := context.WithCancelCause(deadline)
				defer cancel(nil)
				want := error(context.Canceled)
				if mode == "deadline" {
					var end context.CancelFunc
					ctx, end = context.WithTimeout(ctx, 300*time.Millisecond)
					defer end()
					want = context.DeadlineExceeded
				} else if mode == "cause" {
					want = errors.New("business canceled connection")
				}
				result := make(chan error, 1)
				go func() {
					var c *syncnet.Client
					var err error
					if resume {
						c, err = syncnet.DialResume(ctx, ln.Addr().String(), "alpha", tc, arena.Model{}, syncnet.ResumeToken{1})
					} else {
						c, err = syncnet.Dial(ctx, ln.Addr().String(), "alpha", tc, arena.Model{})
					}
					if c != nil {
						c.Close()
					}
					result <- err
				}()
				conn, err := ln.Accept(deadline)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.CloseWithError(10, "test end")
				st, err := conn.AcceptStream(deadline)
				if err != nil {
					t.Fatal(err)
				}
				st.SetReadDeadline(time.Now().Add(time.Second))
				var prefix [4]byte
				if _, err := io.ReadFull(st, prefix[:]); err != nil {
					t.Fatal(err)
				}
				// Read the complete request; deliberately withhold the full state.
				if n := binary.LittleEndian.Uint32(prefix[:]); n != 41 {
					t.Fatal("unexpected request length", n)
				}
				if _, err := io.CopyN(io.Discard, st, 41); err != nil {
					t.Fatal(err)
				}
				if mode != "deadline" {
					cancel(want)
				}
				select {
				case err := <-result:
					if !errors.Is(err, want) {
						t.Fatalf("wanted %v, got %v", want, err)
					}
				case <-deadline.Done():
					t.Fatal("canceled connection did not return")
				}
			})
		}
	}
}

type cancelDuringInitialization struct {
	arena.Model
	once   sync.Once
	cancel context.CancelFunc
}

func (m *cancelDuringInitialization) ValidateState(b []byte) error {
	m.once.Do(m.cancel)
	return m.Model.ValidateState(b)
}

func TestDialCancellationDuringInitialization(t *testing.T) {
	for _, resuming := range []bool{false, true} {
		t.Run(map[bool]string{false: "join", true: "resume"}[resuming], func(t *testing.T) {
			f := setup(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			model := &cancelDuringInitialization{cancel: cancel}
			var c, old *syncnet.Client
			var err error
			if resuming {
				old = connect(t, f, f.s.Addr().String(), "alpha")
				c, err = syncnet.DialResume(ctx, f.s.Addr().String(), "alpha", f.tls, model, old.ResumeToken())
			} else {
				c, err = syncnet.Dial(ctx, f.s.Addr().String(), "alpha", f.tls, model)
			}
			if c != nil {
				c.Disconnect()
				t.Error("canceled initialization returned a client")
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("canceled initialization returned %v", err)
			}
			if resuming {
				// Cancellation after takeover must not invalidate the stable token.
				next := resume(t, f, old)
				if next.ResumeToken() != old.ResumeToken() {
					t.Fatal("cancellation changed resume credential")
				}
			}
		})
	}
}
