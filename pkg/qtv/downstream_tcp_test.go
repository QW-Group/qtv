package qtv

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/soheilhy/cmux"
)

// Native writes must not replace the cmux reader, which owns the sniffed prefix.
func TestDownstreamTCPWriterPreservesMuxInput(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := cmux.New(listener)
	qtvListener := mux.Match(cmux.PrefixMatcher("QTV"))
	serveDone := make(chan error, 1)
	go func() { serveDone <- mux.Serve() }()
	defer func() {
		mux.Close()
		listener.Close()
		select {
		case <-serveDone:
		case <-time.After(time.Second):
			t.Error("mux did not stop")
		}
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(3 * time.Second))
	header := []byte("QTV\nVERSION: 1\n\n")
	if _, err := client.Write(header); err != nil {
		t.Fatal(err)
	}
	accepted, err := qtvListener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	accepted.SetDeadline(time.Now().Add(3 * time.Second))
	ds := &dStream{conn: accepted, wb: mustByteQueue(192*1024, 16*1024), ioECh: make(chan error, 2), writeReady: make(chan struct{}, 1)}
	data := tcpQueuePayload(144000)
	ds.wb.WriteFull(append([]byte("skip!"), data...))
	ds.wb.Discard(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ds.ioWriter(ctx) }()
	defer func() {
		cancel()
		accepted.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("downstream writer did not stop")
		}
	}()
	gotHeader := make([]byte, len(header))
	if _, err := io.ReadFull(ds.conn, gotHeader); err != nil || !bytes.Equal(gotHeader, header) {
		t.Fatal("mux sniffed input was lost", err)
	}
	got := make([]byte, len(data))
	if _, err := io.ReadFull(client, got); err != nil || !bytes.Equal(got, data) {
		t.Fatal("mux output was corrupted", err)
	}
}
