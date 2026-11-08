package qtv

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"net"
	"testing"
	"time"
)

func TestUpstreamOutputQueueOwnershipAndSkip(t *testing.T) {
	q := queueTestQTV()
	us, e := newUStream(q, nil, "tcp:localhost:1234", "test", 1, uStreamOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer us.cancel()
	us.wb = mustByteQueue(32, 4)
	us.state = usParsingConnection
	if e := us.sendClcStringCmdf("hello"); e != nil {
		t.Fatal(e)
	}
	want := append([]byte{}, us.qp.w.Bytes()...)
	us.qp.w.Clear().PutString("destroy scratch")
	if !bytes.Equal(queueBytes(us.wb), want) {
		t.Fatal("retained caller scratch")
	}
	us.wb.WriteFull(make([]byte, us.wb.Available()))
	before := queueBytes(us.wb)
	if e := us.sendClcStringCmdf("skip"); e == nil || getUStreamSendNetMsgError(e) != nil || !bytes.Equal(queueBytes(us.wb), before) {
		t.Fatal("changed skip-on-full policy", e)
	}
	if e := us.setState(usInitial); e != nil {
		t.Fatal(e)
	}
	if us.wb.Length() != 0 {
		t.Fatal("stale reconnect output")
	}
}

// Only the occupied-byte counter is needed to exercise the int32 boundary;
// allocating a multi-GiB command payload would obscure this preflight check.
func TestUpstreamOutputQueueLargeBudgetSkip(t *testing.T) {
	for _, available := range []int{0, 2} {
		us, err := newUStream(queueTestQTV(), nil, "tcp:localhost:1234", "test", 1, uStreamOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer us.cancel()
		us.state = usParsingConnection
		us.wb = mustByteQueue(math.MaxInt32, 4096)
		us.wb.length = math.MaxInt32 - available
		before := us.wb.Length()
		if err := us.sendClcStringCmdf("skip"); err == nil || getUStreamSendNetMsgError(err) != nil || us.wb.Length() != before {
			t.Fatal("large queue overflow must remain a nonfatal skip", available, err)
		}
	}
}

func TestUpstreamOutputQueueTCPHandshake(t *testing.T) {
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	q := queueTestQTV()
	us, e := newUStream(q, nil, "tcp:"+listener.Addr().String(), "test", 1, uStreamOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer us.cancel()
	if us.wb.Capacity() != 32768 || len(us.wb.blocks) != 0 {
		t.Fatal("output defaults/lazy allocation")
	}
	if e := us.io.Open(context.Background()); e != nil {
		t.Fatal(e)
	}
	peer, e := listener.Accept()
	if e != nil {
		t.Fatal(e)
	}
	defer peer.Close()
	peer.SetDeadline(time.Now().Add(2 * time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	us.ioECh = make(chan error, 2)
	done := make(chan error, 1)
	go func() { done <- us.ioWriter(ctx) }()
	if e := us.sendQTVConnectionRequest("NONE", ""); e != nil {
		t.Fatal(e)
	}
	var got []byte
	b := make([]byte, 1024)
	for !bytes.Contains(got, []byte("\n\n")) {
		n, e := peer.Read(b)
		if e != nil {
			t.Fatal(e)
		}
		got = append(got, b[:n]...)
	}
	if !bytes.HasPrefix(got, []byte("QTV\nVERSION: 1\nRECEIVE\n")) || !bytes.Contains(got, []byte("AUTH: NONE\n")) {
		t.Fatal(string(got))
	}
	us.state = usParsingConnection
	if e := us.sendClcStringCmdf("say tcp"); e != nil {
		t.Fatal(e)
	}
	prefix := make([]byte, 2)
	if _, e := io.ReadFull(peer, prefix); e != nil {
		t.Fatal(e)
	}
	body := make([]byte, int(binary.LittleEndian.Uint16(prefix))-2)
	if _, e := io.ReadFull(peer, body); e != nil {
		t.Fatal(e)
	}
	if string(body[1:]) != "say tcp\x00" {
		t.Fatal(body)
	}
	cancel()
	us.io.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer leaked")
	}
	us.wb.Reset()
}

func TestUpstreamOutputQueueAuth(t *testing.T) {
	q := queueTestQTV()
	us, e := newUStream(q, nil, "tcp:localhost:1234", "test", 1, uStreamOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer us.cancel()
	for _, method := range []string{"PLAIN", "SHA2_512", "SHA3_512"} {
		us.wb.Reset()
		if e := us.sendQTVConnectionRequest(method, string(bytes.Repeat([]byte{'c'}, maxAuthChallengeSize))); e != nil {
			t.Fatal(e)
		}
		if !bytes.Contains(queueBytes(us.wb), []byte("AUTH: "+method+"\nPASSWORD: \"")) {
			t.Fatal(method)
		}
	}
}
