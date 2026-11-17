package qtv

import (
	"context"
	"io"
	"net"
	"testing"
	"testing/synctest"
)

// A channel-backed listener lets the test observe whether cmux accepted the
// next socket without relying on the timing of a real TCP accept.
type ingressTestListener chan net.Conn

func (l ingressTestListener) Accept() (net.Conn, error) {
	conn, ok := <-l
	if !ok {
		return nil, net.ErrClosed
	}
	return conn, nil
}

func (l ingressTestListener) Close() error {
	close(l)
	return nil
}

func (l ingressTestListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestTCPMuxHoldsSlotUntilHTTPAccept(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		root := make(ingressTestListener, 2)
		mux, _, httpListener := newTCPMux(ctx, root, true, 1)
		go mux.Serve()
		defer func() {
			cancel()
			mux.Close()
			root.Close()
		}()

		first, client := net.Pipe()
		defer client.Close()
		second, other := net.Pipe()
		defer second.Close()
		defer other.Close()
		root <- first
		root <- second
		const prefix = "GET "
		if _, err := io.WriteString(client, prefix); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(root) != 1 {
			t.Fatal("cmux accepted the next socket before HTTP accepted the first")
		}

		conn, err := httpListener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		var got [len(prefix)]byte
		if _, err := io.ReadFull(conn, got[:]); err != nil || string(got[:]) != prefix {
			t.Fatalf("HTTP prefix = %q, %v; want %q", got, err, prefix)
		}
		synctest.Wait()
		if len(root) != 0 {
			t.Fatal("HTTP accepted the first socket but its ingress slot is still held")
		}
	})
}
