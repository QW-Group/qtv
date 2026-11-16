package qtv

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"golang.org/x/crypto/sha3"
)

func TestDownstreamTCPWriterPreservesBufferedInput(t *testing.T) {
	q, dial := serveReverseTestQTV(t)
	const password = "viewer password"
	q.qvs.Set("qtv_password", password)
	q.setDemoList(nil)
	client, reader := dial()
	extension := fmt.Sprintf("%s: %d\n", qtvEzQuakeExt, qtvEzQuakeExtDownload)
	// The extension starts with the bytes sniffed by cmux. The second header
	// selects a different command so losing classifier read-ahead is observable.
	reverseTestWrite(t, client, extension+"QTV\nVERSION: 1\nDEMOLIST\nAUTH: SHA3_512\n\nSOURCELIST\n")
	header := reverseTestReadHeader(t, reader)
	challenge, ok := strings.CutPrefix(header, "QTVSV 1\n"+extension+"AUTH: SHA3_512\nCHALLENGE: ")
	if !ok {
		t.Fatalf("viewer challenge was lost: %q", header)
	}

	q.dss.mu.Lock()
	ds := q.dss.stream[1]
	q.dss.mu.Unlock()
	if ds == nil {
		t.Fatal("TCP viewer was not registered")
	}
	// The queue gathers multiple blocks only for a native TCP writer.
	if _, ok := ds.writer.(*net.TCPConn); !ok {
		t.Fatalf("viewer writer is %T, want *net.TCPConn", ds.writer)
	}
	data := tcpQueuePayload(144000)
	if _, err := ds.wb.WriteFull(data); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(data))
	if _, err := io.ReadFull(reader, got); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("multi-block TCP output was corrupted: %v", err)
	}

	response := sha3.Sum512([]byte(strings.TrimSuffix(challenge, "\n\n") + password))
	reverseTestWrite(t, client, fmt.Sprintf("PASSWORD: %x\n\n", response))
	if got := reverseTestReadHeader(t, reader); !strings.Contains(got, "PERROR: no sources currently available\n") {
		t.Fatalf("buffered viewer authentication did not complete: %q", got)
	}
}
