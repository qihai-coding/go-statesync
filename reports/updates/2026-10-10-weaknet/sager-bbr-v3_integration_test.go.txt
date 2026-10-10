package statesync_test

import (
	"context"
	"errors"
	"testing"
	"time"

	syncnet "github.com/qihai-coding/go-statesync"
	"github.com/sagernet/quic-go"
)

func TestServerRejectsVersionTwoConnections(t *testing.T) {
	f := setup(t)
	for _, alpn := range []string{"statesync/2", syncnet.ALPN} {
		t.Run(alpn, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			tls := f.tls.Clone()
			tls.NextProtos = []string{alpn}
			conn, err := quic.DialAddr(ctx, f.s.Addr().String(), tls, &quic.Config{EnableDatagrams: true})
			if alpn == "statesync/2" {
				if conn != nil {
					conn.CloseWithError(10, "test end")
				}
				if err == nil {
					t.Fatal("version two ALPN accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseWithError(10, "test end")
			stream, err := conn.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			join := append([]byte{2, 1, 5, 0, 'a', 'l', 'p', 'h', 'a'}, make([]byte, 32)...)
			rawWrite(t, stream, join)
			select {
			case <-conn.Context().Done():
				var ended *quic.ApplicationError
				if !errors.As(context.Cause(conn.Context()), &ended) || ended.ErrorCode != 4 {
					t.Fatal("old protocol did not receive protocol rejection", context.Cause(conn.Context()))
				}
			case <-ctx.Done():
				t.Fatal("version two request was not rejected")
			}
		})
	}
}
