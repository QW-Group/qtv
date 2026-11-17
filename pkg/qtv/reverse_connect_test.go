package qtv

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/sha3"
)

const (
	testReverseSecret = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	testReverseHello  = "QTV\nVERSION: 1\nREVERSE\n\n"
	testReverseBegin  = "QTVSV 1\nBEGIN\n\n"
)

func serveReverseTestQTV(t *testing.T) (*QTV, func() (net.Conn, *bufio.Reader)) {
	t.Helper()
	return startReverseTestQTV(t, nil)
}

func startReverseTestQTV(t *testing.T, wrap func(net.Listener) net.Listener) (*QTV, func() (net.Conn, *bufio.Reader)) {
	t.Helper()
	q := &QTV{}
	q.cmd = newQCmd(q)
	q.qvs = newQVarStorage(q)
	q.qvs.Reg("network", "4")
	q.qvs.Reg("address", "proxy.example.net:28000")
	q.qvs.Reg("hostname", "Proxy")
	ctx, cancel := context.WithCancel(context.Background())
	q.ctx = ctx
	q.tick = sync.NewCond(&sync.Mutex{})
	q.uss = newUStreamStorage(q)
	q.dss = newDStreamStorage(q)
	q.qvs.Set("qtv_reverse_secret", testReverseSecret)
	q.qvs.Set("parse_delay", "0")
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux, qtvListener, _ := newTCPMux(ctx, l, false, qtvMaxClients)
	if wrap != nil {
		qtvListener = wrap(qtvListener)
	}
	done := make(chan struct{}, 4)
	stopTicks := make(chan struct{})
	go func() { _ = mux.Serve(); done <- struct{}{} }()
	go func() { _ = q.dss.serve(qtvListener); done <- struct{}{} }()
	go func() { _ = q.uss.serve(); done <- struct{}{} }()
	go func() {
		ticks := time.NewTicker(time.Millisecond)
		defer ticks.Stop()
		defer func() { done <- struct{}{} }()
		for {
			select {
			case <-ticks.C:
				q.tick.Broadcast()
			case <-stopTicks:
				return
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		mux.Close()
		l.Close()
		for i := 0; i < 3; i++ {
			select {
			case <-done:
			case <-time.After(4 * time.Second):
				t.Error("test server did not stop")
			}
		}
		close(stopTicks)
		<-done
	})
	return q, func() (net.Conn, *bufio.Reader) {
		c, err := net.Dial("tcp4", l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		return c, bufio.NewReader(c)
	}
}

func reverseTestWrite(t *testing.T, c net.Conn, s string) {
	t.Helper()
	if _, err := io.WriteString(c, s); err != nil {
		t.Fatal(err)
	}
}

func reverseTestReadHeader(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	var b strings.Builder
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("reading QTV response after %q: %v", b.String(), err)
		}
		b.WriteString(line)
		if line == "\n" {
			return b.String()
		}
	}
}

func reverseTestChallenge(t *testing.T, c net.Conn, r *bufio.Reader) string {
	t.Helper()
	for _, part := range []string{"QTV\nVER", "SION: 1\nRE", "VERSE\n", "\n"} {
		reverseTestWrite(t, c, part)
	}
	header := reverseTestReadHeader(t, r)
	hexChallenge, ok := strings.CutPrefix(header, "QTVSV 1\nAUTH: HMAC-SHA3-256\nCHALLENGE: ")
	if !ok {
		t.Fatalf("reverse source received no challenge: %q", header)
	}
	return strings.TrimSuffix(hexChallenge, "\n\n")
}

func reverseTestHMAC(t *testing.T, hexChallenge string) string {
	t.Helper()
	challenge, err := hex.DecodeString(hexChallenge)
	if err != nil || len(challenge) != 32 {
		t.Fatalf("invalid challenge: %q", hexChallenge)
	}
	key, _ := hex.DecodeString(testReverseSecret)
	mac := hmac.New(sha3.New256, key)
	mac.Write(challenge)
	return hex.EncodeToString(mac.Sum(nil))
}

func reverseTestProof(hexMAC string) string {
	return "QTV\nVERSION: 1\nREVERSE\nAUTH: HMAC-SHA3-256\nRESPONSE: " + hexMAC + "\n\n"
}

func reverseTestRejected(t *testing.T, r *bufio.Reader) {
	t.Helper()
	s, err := io.ReadAll(r)
	if err != nil && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("reverse connection was not closed: %v", err)
	}
	if strings.Contains(string(s), "\nREVERSED\n") || strings.Contains(string(s), "\nCHALLENGE:") {
		t.Fatalf("unexpected acceptance: %q", s)
	}
}

func reverseTestWaitCount(t *testing.T, count func() int, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for count() != want {
		if time.Now().After(deadline) {
			t.Fatalf("stream count = %d, want %d", count(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReverseConnectHandsOffToLiveUpstream(t *testing.T) {
	q, dial := serveReverseTestQTV(t)
	q.qvs.Set("maxclients", "1")
	c, r := dial()
	proof := reverseTestHMAC(t, reverseTestChallenge(t, c, r))
	if got := reverseTestHMAC(t, "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"); got != "0b6c45effc0ba0ac16185811d2b848a557bfcfb3c6d3a6584447ef1470d350b9" {
		t.Fatal("sender did not match the agreed HMAC vector")
	}
	// Include BEGIN and the first MVD record in the same write as the proof so
	// a test proxy that loses its handshake read-ahead drops the record as well.
	record := reverseTestStuffText("fullserverinfo \"\\hostname\\game server\\hostport\\game.example.net:27500\"\n")
	wire := reverseTestProof(proof) + testReverseBegin + string(record)
	reverseTestWrite(t, c, wire[:78])
	reverseTestWrite(t, c, wire[78:])
	header := reverseTestReadHeader(t, r)
	for _, s := range []string{"QTVSV 1\nVERSION: 1\nREVERSED\n", "\\name\\Proxy", "\\address\\proxy.example.net:28000", "\\streamid\\1"} {
		if !strings.Contains(header, s) {
			t.Fatalf("missing %q in %q", s, header)
		}
	}
	reverseTestWaitPending(t, q.dss, 0)
	viewer, viewerReader := dial()
	reverseTestWrite(t, viewer, "QTV\nVERSION: 1\nSOURCELIST\n\n")
	if got := reverseTestReadHeader(t, viewerReader); !strings.Contains(got, "ASOURCE: 1:") {
		t.Fatalf("ordinary source-list request lost the reverse source: %q", got)
	}
	viewer.Close()
	reverseTestWaitCount(t, q.dss.count, 0)

	q.uss.mu.Lock()
	us := q.uss.getStreamById(1)
	q.uss.mu.Unlock()
	if us == nil {
		t.Fatal("authenticated source was not registered")
	}
	deadline := time.Now().Add(time.Second)
	for us.qp.remoteHostName.Load() != "game server" {
		if time.Now().After(deadline) {
			t.Fatal("MVD already buffered during handshake was not parsed")
		}
		time.Sleep(time.Millisecond)
	}

	viewer, viewerReader = dial()
	reverseTestWrite(t, viewer, "QTV\nVERSION: 1\nSOURCE: 1\nUSERINFO: \"\\name\\Viewer\"\n\n")
	if got := reverseTestReadHeader(t, viewerReader); !strings.Contains(got, "\nBEGIN: ") {
		t.Fatalf("viewer could not open the reverse source: %q", got)
	}
	reverseTestWaitCount(t, q.dss.count, 1)
	// The linked viewer triggers the ordinary viewer-list command on this socket.
	length := make([]byte, 2)
	if _, err := io.ReadFull(r, length); err != nil {
		t.Fatal(err)
	}
	command := make([]byte, binary.LittleEndian.Uint16(length)-2)
	if _, err := io.ReadFull(r, command); err != nil || !strings.Contains(string(command), "qul 1 ") {
		t.Fatalf("no viewer-list command after handoff: %q, %v", command, err)
	}
	c.Close()
	reverseTestWaitCount(t, q.uss.count, 0)
}

func TestReverseConnectRejectsInvalidHandshake(t *testing.T) {
	for _, tc := range []struct{ name, hello string }{
		{"repeated version", "QTV\nVERSION: 1\nVERSION: 1\nREVERSE\n\n"},
		{"repeated reverse", "QTV\nVERSION: 1\nREVERSE\nREVERSE\n\n"},
		{"viewer command", "QTV\nVERSION: 1\nSOURCELIST\nREVERSE\n\n"},
		{"proof before challenge", reverseTestProof(strings.Repeat("0", 64))},
		{"out of phase", "QTV\nVERSION: 1\nREVERSE\nBEGIN\n\n"},
		{"large aggregate", "QTV\nVERSION: 1\n" + strings.Repeat("PRINT: "+strings.Repeat("x", 100)+"\n", 350) + "REVERSE\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, dial := serveReverseTestQTV(t)
			c, r := dial()
			reverseTestWrite(t, c, tc.hello)
			reverseTestRejected(t, r)
			reverseTestWaitPending(t, q.dss, 0)
			reverseTestWaitCount(t, q.uss.count, 0)
		})
	}
}

func TestReverseReconnectWithFullViewerLimit(t *testing.T) {
	q, dial := serveReverseTestQTV(t)
	q.qvs.Set("maxclients", "1")
	q.qvs.Set("maxservers", "2")
	connectSource := func() net.Conn {
		c, r := dial()
		proof := reverseTestHMAC(t, reverseTestChallenge(t, c, r))
		reverseTestWrite(t, c, reverseTestProof(proof)+testReverseBegin)
		if got := reverseTestReadHeader(t, r); !strings.Contains(got, "\nREVERSED\n") {
			t.Fatalf("source was not registered: %q", got)
		}
		return c
	}
	connectSource()
	reconnecting := connectSource()
	viewer, viewerReader := dial()
	reverseTestWrite(t, viewer, "QTV\nVERSION: 1\nSOURCE: 1\n\n")
	if got := reverseTestReadHeader(t, viewerReader); !strings.Contains(got, "\nBEGIN: ") {
		t.Fatalf("viewer was not admitted: %q", got)
	}
	reverseTestWaitCount(t, q.dss.count, 1)

	reconnecting.Close()
	reverseTestWaitCount(t, q.uss.count, 1)
	connectSource()
	reverseTestWaitCount(t, q.uss.count, 2)

	extra, extraReader := dial()
	reverseTestWrite(t, extra, "QTV\nVERSION: 1\nSOURCE: 1\n\n")
	got, err := io.ReadAll(extraReader)
	if len(got) != 0 || (err != nil && !errors.Is(err, syscall.ECONNRESET)) {
		t.Fatalf("extra viewer was not closed without a reply: %q, %v", got, err)
	}
	reverseTestWaitCount(t, q.dss.count, 1)
}

func TestReverseConnectSharesHeaderBudgetAcrossChallenge(t *testing.T) {
	q, dial := serveReverseTestQTV(t)
	c, r := dial()
	paddedVersion := "VERSION: " + strings.Repeat(" ", 20*1024) + "1\n"
	first := "QTV\n" + paddedVersion + "REVERSE\n\n"
	reverseTestWrite(t, c, first)
	header := reverseTestReadHeader(t, r)
	challenge, ok := strings.CutPrefix(header, "QTVSV 1\nAUTH: HMAC-SHA3-256\nCHALLENGE: ")
	if !ok {
		t.Fatalf("first phase should fit the header budget: %q", header)
	}
	proof := reverseTestHMAC(t, strings.TrimSuffix(challenge, "\n\n"))
	reverseTestWrite(t, c, strings.Replace(reverseTestProof(proof), "VERSION: 1\n", paddedVersion, 1))
	reverseTestRejected(t, r)
	reverseTestWaitCount(t, q.uss.count, 0)
}

func TestReverseConnectRejectsBadAndReplayedProof(t *testing.T) {
	q, dial := serveReverseTestQTV(t)
	for _, tc := range []string{"wrong response", "response for another socket", "duplicate response"} {
		t.Run(tc, func(t *testing.T) {
			c, r := dial()
			challenge := reverseTestChallenge(t, c, r)
			proof := reverseTestProof(reverseTestHMAC(t, challenge))
			switch tc {
			case "wrong response":
				proof = reverseTestProof(strings.Repeat("0", 64))
			case "response for another socket":
				other, otherReader := dial()
				if challenge == reverseTestChallenge(t, other, otherReader) {
					t.Fatal("reused challenge")
				}
				c, r = other, otherReader
			case "duplicate response":
				proof = strings.TrimSuffix(proof, "\n") + "RESPONSE: " + strings.Repeat("0", 64) + "\n\n"
			}
			reverseTestWrite(t, c, proof)
			reverseTestRejected(t, r)
			reverseTestWaitCount(t, q.uss.count, 0)
		})
	}
}

func TestReverseConnectRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct{ name, variable, value string }{
		{"disabled", "qtv_reverse_secret", ""},
		{"invalid secret", "qtv_reverse_secret", "invalid"},
		{"no public address", "address", ""},
		{"oversized public address", "address", strings.Repeat("x", 64) + ":28000"},
		{"invalid port", "address", "proxy.example.net:65536"},
		{"control in proxy name", "hostname", "Proxy\x10 QA"},
		{"DEL in proxy name", "hostname", "Proxy\x7f QA"},
		{"control in public address", "address", "proxy\x10.example.net:28000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, dial := serveReverseTestQTV(t)
			q.qvs.Set(tc.variable, tc.value)
			c, r := dial()
			reverseTestWrite(t, c, testReverseHello)
			if got := reverseTestReadHeader(t, r); !strings.Contains(got, "\nPERROR: ") {
				t.Fatalf("invalid proxy configuration was accepted: %q", got)
			}
			reverseTestRejected(t, r)
		})
	}
}

func TestReverseConnectReservesLastUpstreamSlot(t *testing.T) {
	q, dial := serveReverseTestQTV(t)
	q.qvs.Set("maxservers", "1")
	first, firstReader := dial()
	firstProof := reverseTestProof(reverseTestHMAC(t, reverseTestChallenge(t, first, firstReader)))
	second, secondReader := dial()
	secondProof := reverseTestProof(reverseTestHMAC(t, reverseTestChallenge(t, second, secondReader)))
	reverseTestWrite(t, first, firstProof)
	reverseTestWrite(t, second, secondProof)
	accepted := 0
	for _, r := range []*bufio.Reader{firstReader, secondReader} {
		if header := reverseTestReadHeader(t, r); strings.Contains(header, "\nREVERSED\n") {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d sources for one slot", accepted)
	}
	first.Close()
	second.Close()
	reverseTestWaitCount(t, q.uss.count, 0)
}

// Enable tracing in a dedicated test process. Zerolog's application logger is
// global and ordinary stream goroutines can still be finishing after cleanup.
func reverseTestTrace(t *testing.T, run func()) []byte {
	t.Helper()
	const childEnv = "QTV_REVERSE_TEST_TRACE"
	if os.Getenv(childEnv) == t.Name() {
		log.Logger = zerolog.New(os.Stderr)
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
		run()
		return nil
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+t.Name()+"$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), childEnv+"="+t.Name())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("trace test failed: %v\n%s", err, out)
	}
	return out
}

func TestReverseConnectDoesNotLogCredentials(t *testing.T) {
	proof := strings.Repeat("f", 64)
	out := reverseTestTrace(t, func() {
		q, dial := serveReverseTestQTV(t)
		c, r := dial()
		reverseTestChallenge(t, c, r)
		reverseTestWrite(t, c, reverseTestProof(proof))
		reverseTestRejected(t, r)
		reverseTestWaitPending(t, q.dss, 0)
		if q.qvs.ExpandString("${qtv_reverse_secret}") == testReverseSecret {
			t.Fatal("secret can be echoed through console expansion")
		}
		if _, err := q.qvs.CommandIsVar(q, newQCmdArgs([]string{"qtv_reverse_secret"})); err != nil {
			t.Fatal(err)
		}
	})
	if bytes.Contains(out, []byte(proof)) || bytes.Contains(out, []byte(testReverseSecret)) {
		t.Fatal("reverse credentials appeared in diagnostics")
	}
}

func reverseTestStuffText(s string) []byte {
	// MVD dem_all with a NUL-terminated svc_stufftext message.
	record := binary.LittleEndian.AppendUint32([]byte{0, 6}, uint32(len(s)+2))
	return append(append(append(record, 9), s...), 0)
}

func reverseTestWaitPending(t *testing.T, dss *dStreamStorage, want int) {
	t.Helper()
	reverseTestWaitCount(t, func() int {
		dss.mu.Lock()
		defer dss.mu.Unlock()
		return dss.pending
	}, want)
}

func TestReverseConnectBoundsPendingHandshakes(t *testing.T) {
	q, dial := serveReverseTestQTV(t)
	q.qvs.Set("maxclients", "1")
	connections := make([]net.Conn, maxPendingQTVHandshakes)
	for i := range connections {
		c, _ := dial()
		reverseTestWrite(t, c, "QTV\nVERSION: 1\n")
		connections[i] = c
	}
	reverseTestWaitPending(t, q.dss, maxPendingQTVHandshakes)
	if got := q.dss.count(); got != 0 {
		t.Fatalf("pending handshakes counted as %d viewers", got)
	}

	extra, extraReader := dial()
	reverseTestWrite(t, extra, testReverseHello)
	reverseTestRejected(t, extraReader)

	connections[0].Close()
	reverseTestWaitPending(t, q.dss, maxPendingQTVHandshakes-1)

	q.qvs.Set("qtv_password", "viewer only")
	viewer, viewerReader := dial()
	reverseTestWrite(t, viewer, "QTV\nVERSION: 1\nSOURCELIST\nAUTH: SHA3_512\n\n")
	if got := reverseTestReadHeader(t, viewerReader); !strings.Contains(got, "\nCHALLENGE: ") {
		t.Fatalf("pending handshakes blocked a free viewer slot: %q", got)
	}
	reverseTestWaitCount(t, q.dss.count, 1)
	viewer.Close()
	reverseTestWaitCount(t, q.dss.count, 0)

	replacement, replacementReader := dial()
	reverseTestChallenge(t, replacement, replacementReader)
	reverseTestWaitPending(t, q.dss, maxPendingQTVHandshakes)
}

func TestReverseConnectBoundsHandshakeTime(t *testing.T) {
	q, dial := serveReverseTestQTV(t)
	c, r := dial()
	start := time.Now()
	reverseTestWrite(t, c, "QTV\nVERSION: 1\n")
	reverseTestWaitPending(t, q.dss, 1)
	// Spend more than half of the full deadline before even identifying the
	// reverse request. A fresh deadline at the challenge would miss this cap.
	time.Sleep(3 * time.Second)
	reverseTestWrite(t, c, "REVERSE\n\n")
	if got := reverseTestReadHeader(t, r); !strings.Contains(got, "\nCHALLENGE: ") {
		t.Fatalf("partial handshake did not finish first phase: %q", got)
	}
	if err := c.SetReadDeadline(start.Add(7 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reverseTestRejected(t, r)
	reverseTestWaitPending(t, q.dss, 0)
}

func TestReverseConnectBeginHasAbsoluteDeadline(t *testing.T) {
	q, dial := serveReverseTestQTV(t)
	q.qvs.Set("maxservers", "1")
	c, r := dial()
	proof := reverseTestHMAC(t, reverseTestChallenge(t, c, r))
	reverseTestWrite(t, c, reverseTestProof(proof))
	if got := reverseTestReadHeader(t, r); !strings.Contains(got, "\nREVERSED\n") {
		t.Fatalf("source was not authenticated: %q", got)
	}
	start := time.Now()
	reverseTestWaitCount(t, q.uss.count, 1)
	time.Sleep(3 * time.Second)
	reverseTestWrite(t, c, "QTVSV 1\nBE")
	if err := c.SetReadDeadline(start.Add(7 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reverseTestRejected(t, r)
	reverseTestWaitCount(t, q.uss.count, 0)
}

func TestReverseConnectRejectsHeadersBeforeBegin(t *testing.T) {
	q, dial := serveReverseTestQTV(t)
	c, r := dial()
	proof := reverseTestHMAC(t, reverseTestChallenge(t, c, r))
	reverseTestWrite(t, c, reverseTestProof(proof))
	if got := reverseTestReadHeader(t, r); !strings.Contains(got, "\nREVERSED\n") {
		t.Fatalf("source was not authenticated: %q", got)
	}
	reverseTestWrite(t, c, "QTVSV 1\nAUTH: PLAIN\nBEGIN\n\n")
	out, err := io.ReadAll(r)
	if err != nil || len(out) != 0 {
		t.Fatalf("unexpected activity after invalid BEGIN: %q, %v", out, err)
	}
	reverseTestWaitCount(t, q.uss.count, 0)
}

type reverseReplyListener struct {
	net.Listener
	writeReply func(net.Conn, []byte) (int, error)
}

func (l *reverseReplyListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil && l.writeReply != nil {
		c = reverseReplyConn{Conn: c, writeReply: l.writeReply}
		l.writeReply = nil
	}
	return c, err
}

type reverseReplyConn struct {
	net.Conn
	writeReply func(net.Conn, []byte) (int, error)
}

func (c reverseReplyConn) Write(b []byte) (int, error) {
	if bytes.Contains(b, []byte("\nREVERSED\n")) {
		return c.writeReply(c.Conn, b)
	}
	return c.Conn.Write(b)
}

func TestReverseConnectRegistersIDBeforeSendingReply(t *testing.T) {
	resume := make(chan struct{})
	defer close(resume)
	q, dial := startReverseTestQTV(t, func(l net.Listener) net.Listener {
		return &reverseReplyListener{Listener: l, writeReply: func(c net.Conn, b []byte) (int, error) {
			n, err := c.Write(b)
			// The sender has the ID while QTV is still inside the write call.
			<-resume
			return n, err
		}}
	})
	q.qvs.Set("maxclients", "1")
	source, r := dial()
	proof := reverseTestHMAC(t, reverseTestChallenge(t, source, r))
	reverseTestWrite(t, source, reverseTestProof(proof))
	if got := reverseTestReadHeader(t, r); !strings.Contains(got, "\\streamid\\1") {
		t.Fatalf("source received no stream ID: %q", got)
	}
	reverseTestWaitPending(t, q.dss, 0)
	viewer, viewerReader := dial()
	reverseTestWrite(t, viewer, "QTV\nVERSION: 1\nSOURCE: 1\n\n")
	if got := reverseTestReadHeader(t, viewerReader); !strings.Contains(got, "\nBEGIN: ") {
		t.Fatalf("viewer could not select the stream ID just returned to the source: %q", got)
	}
}

func TestReverseConnectReleasesSlotWhenReplyFails(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	q, dial := startReverseTestQTV(t, func(l net.Listener) net.Listener {
		return &reverseReplyListener{Listener: l, writeReply: func(net.Conn, []byte) (int, error) {
			close(entered)
			<-resume
			return 0, net.ErrClosed
		}}
	})
	q.qvs.Set("maxservers", "1")
	c, r := dial()
	proof := reverseTestHMAC(t, reverseTestChallenge(t, c, r))
	reverseTestWrite(t, c, reverseTestProof(proof))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("did not reach the reverse reply")
	}
	if got := q.uss.count(); got != 1 {
		t.Fatalf("registered upstreams while writing REVERSED: %d, want 1", got)
	}
	viewer, viewerReader := dial()
	reverseTestWrite(t, viewer, "QTV\nVERSION: 1\nSOURCE: 1\n\n")
	if got := reverseTestReadHeader(t, viewerReader); !strings.Contains(got, "\nBEGIN: ") {
		t.Fatalf("viewer could not reach the source while writing REVERSED: %q", got)
	}
	release()
	reverseTestRejected(t, r)
	if _, err := io.ReadAll(viewerReader); err != nil {
		t.Fatalf("viewer did not see EOF after the failed reply: %v", err)
	}
	reverseTestWaitPending(t, q.dss, 0)
	reverseTestWaitCount(t, q.dss.count, 0)
	reverseTestWaitCount(t, q.uss.count, 0)

	c, r = dial()
	proof = reverseTestHMAC(t, reverseTestChallenge(t, c, r))
	reverseTestWrite(t, c, reverseTestProof(proof))
	if got := reverseTestReadHeader(t, r); !strings.Contains(got, "\\streamid\\1") {
		t.Fatalf("failed reply did not free capacity and ID: %q", got)
	}
}

func TestReverseConnectDrainsCompleteMVDBeforeEOF(t *testing.T) {
	out := reverseTestTrace(t, func() {
		q, dial := serveReverseTestQTV(t)
		c, r := dial()
		proof := reverseTestHMAC(t, reverseTestChallenge(t, c, r))
		complete := reverseTestStuffText("reverse-complete-record")
		incomplete := reverseTestStuffText("reverse-incomplete-record")
		reverseTestWrite(t, c, reverseTestProof(proof)+testReverseBegin+string(complete)+string(incomplete[:len(incomplete)-1]))
		if err := c.(*net.TCPConn).CloseWrite(); err != nil {
			t.Fatal(err)
		}
		if got := reverseTestReadHeader(t, r); !strings.Contains(got, "\nREVERSED\n") {
			t.Fatalf("source was not authenticated: %q", got)
		}
		if _, err := io.ReadAll(r); err != nil {
			t.Fatal(err)
		}
		reverseTestWaitCount(t, q.uss.count, 0)
	})
	if out != nil && (!bytes.Contains(out, []byte("reverse-complete-record")) || bytes.Contains(out, []byte("reverse-incomplete-record"))) {
		t.Fatal("did not parse exactly the complete MVD records")
	}
}

func TestReverseConnectPreservesOrdinaryPull(t *testing.T) {
	q, _ := serveReverseTestQTV(t)
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	us, err := q.uss.openAndRun("tcp:"+listener.Addr().String(), true, uStreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := reverseTestReadHeader(t, bufio.NewReader(c))
	if !strings.HasPrefix(got, "QTV\n") || !strings.Contains(got, "\nVERSION: 1\n") || strings.Contains(got, "\nREVERSE\n") {
		t.Fatalf("unexpected pull handshake: %q", got)
	}
	wire := testReverseBegin + string(reverseTestStuffText("fullserverinfo \"\\hostname\\pull server\"\n"))
	reverseTestWrite(t, c, wire)
	deadline := time.Now().Add(time.Second)
	for us.qp.remoteHostName.Load() != "pull server" {
		if time.Now().After(deadline) {
			t.Fatal("ordinary pull MVD was not parsed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReverseConnectPreservesViewerChallenge(t *testing.T) {
	q, dial := serveReverseTestQTV(t)
	q.qvs.Set("qtv_password", "viewer only")
	c, r := dial()
	proof := reverseTestHMAC(t, reverseTestChallenge(t, c, r))
	reverseTestWrite(t, c, reverseTestProof(proof)+testReverseBegin)
	if got := reverseTestReadHeader(t, r); !strings.Contains(got, "\nREVERSED\n") {
		t.Fatalf("reverse authentication used the viewer password: %q", got)
	}

	v, vReader := dial()
	request := "QTV\nVERSION: 1\nSOURCELIST\nAUTH: SHA3_512\n"
	reverseTestWrite(t, v, request+"\n")
	header := reverseTestReadHeader(t, vReader)
	challenge, ok := strings.CutPrefix(header, "QTVSV 1\nAUTH: SHA3_512\nCHALLENGE: ")
	if !ok {
		t.Fatalf("viewer did not receive its normal challenge: %q", header)
	}
	challenge = strings.TrimSuffix(challenge, "\n\n")
	response := sha3.Sum512([]byte(challenge + "viewer only"))
	reverseTestWrite(t, v, fmt.Sprintf("%sPASSWORD: %x\n\n", request, response))
	if got := reverseTestReadHeader(t, vReader); !strings.Contains(got, "ASOURCE: 1:") {
		t.Fatalf("viewer authentication did not complete: %q", got)
	}
}

func TestTCPMuxReleasesClassificationSlots(t *testing.T) {
	for _, tc := range []struct {
		name      string
		allowHTTP bool
	}{
		{"timed out partial prefix", false},
		{"HTTP handoff with open connection", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			root, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			mux, qtvListener, httpListener := newTCPMux(ctx, root, tc.allowHTTP, 1)
			done := make(chan struct{})
			go func() {
				_ = mux.Serve()
				close(done)
			}()
			t.Cleanup(func() {
				cancel()
				mux.Close()
				root.Close()
				<-done
			})
			dial := func(header string) net.Conn {
				c, err := net.Dial("tcp4", root.Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { c.Close() })
				c.SetDeadline(time.Now().Add(time.Second))
				reverseTestWrite(t, c, header)
				return c
			}
			accept := func(l net.Listener) {
				t.Helper()
				connections := make(chan net.Conn, 1)
				go func() {
					c, _ := l.Accept()
					connections <- c
				}()
				select {
				case c := <-connections:
					if c == nil {
						t.Fatal("listener closed before dispatch")
					}
					t.Cleanup(func() { c.Close() })
				case <-time.After(time.Second):
					t.Fatal("classification slot was not released")
				}
			}
			if tc.allowHTTP {
				dial("GET / HTTP/1.0\r\n\r\n")
				accept(httpListener)
			} else {
				partial := dial("QT")
				if _, err := io.ReadAll(partial); err != nil {
					t.Fatalf("partial protocol prefix was not closed: %v", err)
				}
			}
			dial(testReverseHello)
			accept(qtvListener)
		})
	}
}
