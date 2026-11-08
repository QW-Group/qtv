package qtv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// tcpQueuePayload varies with the byte position so swapped/repeated whole blocks
// cannot pass the TCP order and partial-prefix checks.
func tcpQueuePayload(size int) []byte {
	data := make([]byte, size)
	for i := 0; i+4 <= size; i += 4 {
		binary.LittleEndian.PutUint32(data[i:], uint32(i))
	}
	return data
}

// The gathered prefix must span physical blocks without copying their payload,
// and must stop at the IO operation's budget even if more bytes are queued.
func TestByteQueueBorrowedWritePrefix(t *testing.T) {
	q := mustByteQueue(192*1024, 16*1024)
	data := tcpQueuePayload(144000)
	q.WriteFull(append([]byte("skip!"), data...))
	q.Discard(5)
	q.mu.Lock()
	spans := q.appendSpansLocked(nil, 64*1024)
	q.mu.Unlock()
	got := bytes.Join(spans, nil)
	if !bytes.Equal(got, data[:64*1024]) {
		t.Fatal("gathered prefix crossed its budget or changed bytes")
	}
	if len(spans) < 2 || &spans[0][0] != &q.blocks[0].data[5] {
		t.Fatal("prefix does not borrow the original blocks")
	}
	if q.Length() != len(data) {
		t.Fatal("borrowing consumed unsent bytes")
	}
}

// queueTCPPair gives IO tests real sockets so net.Buffers exercises the native
// path rather than its ordinary io.Writer fallback.
func queueTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	a, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	b, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return a, b
}

func TestByteQueueTCPBatchAcrossBlocks(t *testing.T) {
	a, b := queueTCPPair(t)
	q := mustByteQueue(192*1024, 16*1024)
	data := tcpQueuePayload(144000)
	q.WriteFull(append([]byte("skip!"), data...))
	q.Discard(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	permit := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { _, err := q.writeToWithContext(ctx, a, permit); done <- err }()
	defer func() {
		cancel()
		a.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("TCP writer ignored cancellation")
		}
	}()
	b.SetReadDeadline(time.Now().Add(time.Second))
	got := make([]byte, len(data))
	if _, err := io.ReadFull(b, got); err != nil || !bytes.Equal(got, data) {
		t.Fatal("TCP batch", err)
	}
	q.WriteFull([]byte("next"))
	b.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
	var next [4]byte
	if n, err := b.Read(next[:]); n != 0 || err == nil {
		t.Fatal("next batch bypassed permit", n, err)
	}
	permit <- struct{}{}
	b.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(b, next[:]); err != nil || string(next[:]) != "next" {
		t.Fatal("next TCP batch", err)
	}
}

// A timed-out native write may commit a prefix spanning several blocks. Keep
// precisely the unsent suffix queued and report positive progress before error.
func TestByteQueueTCPPartialDeadline(t *testing.T) {
	a, b := queueTCPPair(t)
	if err := a.SetWriteBuffer(1024); err != nil {
		t.Fatal(err)
	}
	if err := b.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	data := tcpQueuePayload(2 * 1024 * 1024)
	q := mustByteQueue(len(data), 16*1024)
	q.WriteFull(data)
	a.SetWriteDeadline(time.Now().Add(80 * time.Millisecond))
	n, err := q.WriteToWithContext(context.Background(), a)
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() || n <= 0 || n >= int64(len(data)) {
		t.Fatal("TCP did not encounter partial backpressure", n, err)
	}
	if q.Length() != len(data)-int(n) || !bytes.Equal(queueBytes(q), data[n:]) {
		t.Fatal("partial native write lost its exact unsent suffix", n, q.Length())
	}
	select {
	case <-q.writeProgress:
	default:
		t.Fatal("partial native write did not report progress")
	}
	a.CloseWrite()
	b.SetReadDeadline(time.Now().Add(3 * time.Second))
	got, readErr := io.ReadAll(b)
	if readErr != nil || int64(len(got)) != n || !bytes.Equal(got, data[:n]) {
		t.Fatal("written prefix differs from committed byte count", n, len(got), readErr)
	}
	q.Reset()
}
