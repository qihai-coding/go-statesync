package main

import (
	"context"
	"errors"
	"testing"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/qihai-coding/go-statesync/arena"
)

func TestReconnectDistinguishesShutdownFromDisconnect(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "temporary", true: "shutdown"}[shutdown], func(t *testing.T) {
			cert, pem, err := syncnet.LocalCertificate()
			if err != nil {
				t.Fatal(err)
			}
			tc, err := syncnet.ClientTLS(pem, "localhost")
			if err != nil {
				t.Fatal(err)
			}
			s, err := syncnet.Listen("127.0.0.1:0", syncnet.ServerTLS(cert), syncnet.DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if _, err = s.CreateRoom("alpha", arena.New()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			c, err := syncnet.Dial(ctx, s.Addr().String(), "alpha", tc, arena.Model{})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if shutdown {
				s.Close()
				select {
				case <-c.Done():
				case <-ctx.Done():
					t.Fatal("server shutdown did not arrive")
				}
				// Terminal errors must return before entering the canceled retry budget.
				cancel()
			} else {
				c.Disconnect()
			}
			next, err := reconnect(ctx, c, s.Addr().String(), "alpha", tc)
			if next != nil {
				defer next.Close()
			}
			if shutdown {
				if next != nil || !errors.Is(err, syncnet.ErrClosed) {
					t.Fatal("shutdown entered retry loop", err)
				}
			} else if err != nil || next == nil || next.Player() != c.Player() || next.ResumeToken() != c.ResumeToken() {
				t.Fatal("temporary disconnect could not resume", err)
			}
		})
	}
}
