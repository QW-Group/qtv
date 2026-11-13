package qtv

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// mustByteQueue creates local fixtures; it is never compiled into production.
func mustByteQueue(limit, block int) *byteQueue {
	q, e := newByteQueue(limit, block)
	if e != nil {
		panic(e)
	}
	return q
}
func queueBytes(q *byteQueue) []byte {
	v := q.Snapshot()
	defer v.release()
	out := make([]byte, 0, v.size)
	for _, p := range v.parts {
		out = append(out, p...)
	}
	return out
}

func TestByteQueueAtomicReuse(t *testing.T) {
	q := mustByteQueue(19, 4)
	payload := []byte("abcdefghijklmnop")
	q.WriteFull(payload)
	payload[0] = '!'
	if n, e := q.WriteFull([]byte("1234")); n != 0 || !errors.Is(e, errByteQueueFull) {
		t.Fatal(n, e)
	}
	q.Discard(13)
	q.WriteFull([]byte("1234567890123456"))
	if got := string(queueBytes(q)); got != "nop1234567890123456" {
		t.Fatal(got)
	}
	if n, e := q.Write([]byte("xyz")); n != 0 || !errors.Is(e, errByteQueueFull) {
		t.Fatal(n, e)
	}
	q.Discard(q.Length())
	if len(q.blocks) != 0 || len(q.spares) > 2 {
		t.Fatal("retained peak")
	}
	q.WriteFull([]byte("ab"))
	if n, e := q.Write(bytes.Repeat([]byte{'x'}, 20)); n != 17 || !errors.Is(e, errByteQueueFull) {
		t.Fatal(n, e)
	}
	q.Reset()
	if q.Length() != 0 || len(q.spares) != 0 {
		t.Fatal("reset")
	}
}

// Notification coalescing must not turn one physical block into the tick budget.
func TestByteQueueInputCoalescesWithoutBlockPacing(t *testing.T) {
	q := mustByteQueue(128*1024, 1024)
	data := bytes.Repeat([]byte("abcd"), 16*1024)
	done := make(chan error, 1)
	go func() {
		n, e := q.ReadFromWithContext(context.Background(), bytes.NewReader(data))
		if n != int64(len(data)) {
			e = io.ErrUnexpectedEOF
		}
		done <- e
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked on notification instead of byte budget")
	}
	if !bytes.Equal(queueBytes(q), data) {
		t.Fatal("input changed")
	}
}

// Even a TCP reader with no consumer permits must accept an entire full-block burst.
func TestByteQueueReadPermitDoesNotPaceFullBlocks(t *testing.T) {
	q := mustByteQueue(128*1024, 1024)
	data := bytes.Repeat([]byte("abcd"), 16*1024)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	n, err := q.ReadFromWithPermit(ctx, bytes.NewReader(data), make(chan struct{}))
	if err != nil || n != int64(len(data)) || !bytes.Equal(queueBytes(q), data) {
		t.Fatal("full reads waited for a consumer permit", n, err)
	}
}

// terminalQueueReader returns a final short prefix together with its terminal error.
type terminalQueueReader struct{ err error }

func (r terminalQueueReader) Read(p []byte) (int, error) {
	return copy(p, "end"), r.err
}

func TestByteQueueReadPermitRetainsTerminalData(t *testing.T) {
	for _, err := range []error{io.EOF, io.ErrClosedPipe} {
		q := mustByteQueue(16, 8)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		n, gotErr := q.ReadFromWithPermit(ctx, terminalQueueReader{err}, make(chan struct{}))
		cancel()
		wantErr := err
		if err == io.EOF {
			wantErr = nil
		}
		if gotErr != wantErr || n != 3 || string(queueBytes(q)) != "end" {
			t.Fatal("terminal read waited for a tick or lost bytes", n, gotErr)
		}
	}
}

func TestByteQueueFullCancellation(t *testing.T) {
	q := mustByteQueue(4, 4)
	q.WriteFull([]byte("full"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := q.ReadFromWithContext(ctx, bytes.NewReader([]byte("x"))); done <- e }()
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("full wait leaked")
	}
	if string(queueBytes(q)) != "full" {
		t.Fatal("cancel changed queued bytes")
	}
}

func TestByteQueueConcurrentOwnership(t *testing.T) {
	q := mustByteQueue(256*1024, 31)
	var wg sync.WaitGroup
	for i := byte(1); i <= 8; i++ {
		wg.Add(1)
		go func(id byte) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if _, e := q.WriteFull(bytes.Repeat([]byte{id}, 23)); e != nil {
					t.Error(e)
				}
			}
		}(i)
	}
	wg.Wait()
	got := queueBytes(q)
	counts := make([]int, 9)
	for len(got) > 0 {
		part := got[:23]
		for _, b := range part {
			if b != part[0] {
				t.Fatal("interleaved atomic write")
			}
		}
		counts[part[0]]++
		got = got[23:]
	}
	for _, n := range counts[1:] {
		if n != 100 {
			t.Fatal(counts)
		}
	}
}

func TestByteQueueBlockedReadOwnerClose(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	q := mustByteQueue(64, 8)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := q.ReadFromWithContext(ctx, a); done <- e }()
	cancel()
	a.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked IO leaked")
	}
	q.Reset()
}

type partialQueueWriter struct {
	out  bytes.Buffer
	zero bool
	fail bool
}

func (w *partialQueueWriter) Write(p []byte) (int, error) {
	if w.zero {
		return 0, nil
	}
	n := min(3, len(p))
	w.out.Write(p[:n])
	if w.fail {
		return n, io.ErrClosedPipe
	}
	return n, nil
}
func TestByteQueueOutputProgress(t *testing.T) {
	q := mustByteQueue(65536, 7)
	data := bytes.Repeat([]byte("abcd"), 4096)
	q.WriteFull(data)
	ctx, cancel := context.WithCancel(context.Background())
	a, b := net.Pipe()
	done := make(chan error, 1)
	go func() { _, e := q.WriteToWithContext(ctx, a); done <- e }()
	received := make([]byte, len(data))
	if _, e := io.ReadFull(b, received); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(data, received) {
		t.Fatal("output changed")
	}
	cancel()
	a.Close()
	b.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("output blocked on progress notification")
	}
	for _, w := range []*partialQueueWriter{{zero: true}, {fail: true}} {
		q.Reset()
		q.WriteFull([]byte("abcdef"))
		_, e := q.WriteToWithContext(context.Background(), w)
		if w.zero && (!errors.Is(e, io.ErrNoProgress) || q.Length() != 6) {
			t.Fatal(e, q.Length())
		}
		if w.fail && (!errors.Is(e, io.ErrClosedPipe) || q.Length() != 3 || w.out.String() != "abc") {
			t.Fatal(e, q.Length())
		}
	}
}

// A blocked Write borrows the head without releasing its bytes or backing block.
func TestByteQueueBorrowedHeadAndSnapshot(t *testing.T) {
	q := mustByteQueue(12, 4)
	q.WriteFull([]byte("abcdefgh"))
	view := q.Snapshot()
	q.WriteFull([]byte("ijkl"))
	if string(view.parts[0]) != "abcd" || view.size != 8 {
		t.Fatal("append invalidated view")
	}
	view.release()
	a, b := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := q.WriteToWithContext(ctx, a); done <- e }()
	deadline := time.Now().Add(time.Second)
	active := false
	for time.Now().Before(deadline) {
		q.mu.Lock()
		active = q.writing
		q.mu.Unlock()
		if active {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !active {
		t.Fatal("writer did not start")
	}
	if n, e := q.WriteFull([]byte("x")); n != 0 || e != errByteQueueFull {
		t.Fatal("borrowed bytes left budget", n, e)
	}
	if e := q.Discard(1); e != errByteQueueBusy {
		t.Fatal("external consumer stole IO head", e)
	}
	cancel()
	a.Close()
	b.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked writer leaked")
	}
	if q.Length() != 12 {
		t.Fatal("failed write consumed data")
	}
	q.Reset()
}

func TestByteQueueOutputBatchCrossesBlocks(t *testing.T) {
	q := mustByteQueue(65536, 1024)
	data := bytes.Repeat([]byte("abcd"), 8192)
	q.WriteFull(data)
	a, b := net.Pipe()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	permit := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { _, e := q.WriteToWithPermit(ctx, a, permit); done <- e }()
	b.SetReadDeadline(time.Now().Add(time.Second))
	got := make([]byte, len(data))
	if _, e := io.ReadFull(b, got); e != nil {
		t.Fatal("batch paced at a block boundary", e)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("batch changed data")
	}
	q.WriteFull([]byte("next"))
	b.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	one := make([]byte, 4)
	if n, e := b.Read(one); n != 0 || e == nil {
		t.Fatal("next batch bypassed permit", n, e)
	}
	b.SetReadDeadline(time.Now().Add(time.Second))
	permit <- struct{}{}
	if _, e := io.ReadFull(b, one); e != nil || string(one) != "next" {
		t.Fatal("next batch", string(one), e)
	}
	cancel()
	a.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("batch permit ignored cancellation")
	}
}

type cancelQueueWriter struct {
	cancel context.CancelFunc
	calls  int
}

func (w *cancelQueueWriter) Write(p []byte) (int, error) { w.calls++; w.cancel(); return len(p), nil }
func TestByteQueueCancelBetweenBlocks(t *testing.T) {
	q := mustByteQueue(65536, 1024)
	q.WriteFull(make([]byte, 65536))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &cancelQueueWriter{cancel: cancel}
	n, e := q.WriteToWithContext(ctx, w)
	if e != context.Canceled || n != 1024 || w.calls != 1 || q.Length() != 65536-1024 {
		t.Fatal(n, e, w.calls, q.Length())
	}
}

// Two-block bursts should reuse bounded storage instead of allocating each tick.
func TestByteQueueReusesTwoBlockBursts(t *testing.T) {
	q := mustByteQueue(64, 16)
	payload := make([]byte, 32)
	q.WriteFull(payload)
	q.Discard(32)
	if n := testing.AllocsPerRun(100, func() { q.WriteFull(payload); q.Discard(32) }); n != 0 {
		t.Fatalf("two-block burst allocations: %g", n)
	}
	q.WriteFull(make([]byte, 64))
	q.Discard(64)
	if len(q.spares) != 2 {
		t.Fatal("spare storage exceeds bounded reserve", len(q.spares))
	}
}

func TestByteQueueStringFlushPartial(t *testing.T) {
	q := mustByteQueue(19, 4)
	q.WriteFull([]byte("prefix!"))
	reader := strings.NewReader("abcdefghijklmno")
	if n, err := reader.WriteTo(q); n != 12 || !errors.Is(err, errByteQueueFull) || reader.Len() != 3 || string(queueBytes(q)) != "prefix!abcdefghijkl" {
		t.Fatal("partial string reply did not retain its exact remainder", n, err, reader.Len(), string(queueBytes(q)))
	}
	q.Discard(8)
	if n, err := reader.WriteTo(q); n != 3 || err != nil || reader.Len() != 0 || string(queueBytes(q)) != "bcdefghijklmno" {
		t.Fatal("continued string reply changed bytes", n, err, reader.Len(), string(queueBytes(q)))
	}
	if n, err := io.WriteString(q, ""); n != 0 || err != nil {
		t.Fatal("empty string write", n, err)
	}
	q.reading = true
	if n, err := io.WriteString(q, "x"); n != 0 || !errors.Is(err, errByteQueueBusy) {
		t.Fatal("string write bypassed exclusive IO producer", n, err)
	}
	q.reading = false
}

// A full queue must reject a large pending string without allocating a copy of
// its unsent suffix; strings.Reader.WriteTo is the actual reply-flush caller.
func TestByteQueueFullStringRejectAllocations(t *testing.T) {
	q := mustByteQueue(64, 16)
	q.WriteFull(make([]byte, 64))
	pending := strings.Repeat("reply", 1<<18)
	allocs := testing.AllocsPerRun(5, func() {
		reader := strings.NewReader(pending)
		if n, err := reader.WriteTo(q); n != 0 || !errors.Is(err, errByteQueueFull) || reader.Len() != len(pending) {
			t.Fatal("full string rejection changed progress", n, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("full queue allocated for pending reply: %g allocations", allocs)
	}
}
