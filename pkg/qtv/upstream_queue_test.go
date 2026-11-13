package qtv

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func queueTestQTV() *QTV {
	q := &QTV{ctx: context.Background()}
	q.cmd = newQCmd(q)
	q.qvs = newQVarStorage(q)
	q.regVars()
	(&uStreamStorage{}).regVars(q)
	return q
}

// finiteQueueSource holds a finite response until the consumer has seen idle
// ticks, so terminal IO can arrive between ticks with a complete queued header.
type finiteQueueSource struct {
	gate       <-chan struct{}
	started    chan struct{}
	terminal   chan struct{}
	data       []byte
	finalError error
	withData   bool
	once       sync.Once
	onClose    func()
}

func (f *finiteQueueSource) Open(context.Context) error  { return nil }
func (f *finiteQueueSource) Close() error                { f.onClose(); return nil }
func (f *finiteQueueSource) Write(p []byte) (int, error) { return len(p), nil }
func (f *finiteQueueSource) Read(p []byte) (int, error) {
	f.once.Do(func() { close(f.started) })
	<-f.gate
	if len(f.data) == 0 {
		close(f.terminal)
		return 0, f.finalError
	}
	n := copy(p, f.data)
	f.data = f.data[n:]
	if len(f.data) == 0 && f.withData {
		close(f.terminal)
		return n, f.finalError
	}
	return n, nil
}

func TestUpstreamQueueDrainsTerminalInput(t *testing.T) {
	record := []byte{0, 6, 2, 0, 0, 0, 32, 7}
	for _, tc := range []struct {
		name           string
		wire           []byte
		terminal       error
		withData       bool
		wantTrack      byte
		wantConnection bool
	}{
		{"data then EOF", append([]byte("QTVSV 1\nBEGIN\n\n"), record...), io.EOF, false, 7, true},
		{"data with EOF", append([]byte("QTVSV 1\nBEGIN\n\n"), record...), io.EOF, true, 7, true},
		{"data with error", append([]byte("QTVSV 1\nBEGIN\n\n"), record...), io.ErrUnexpectedEOF, true, 7, true},
		{"incomplete header", []byte("QTVSV 1\nBEG"), io.EOF, false, 0, false},
		{"complete prefix and partial tail", append(append([]byte("QTVSV 1\nBEGIN\n\n"), record...), record[:7]...), io.EOF, false, 7, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := queueTestQTV()
			q.tick = sync.NewCond(&sync.Mutex{})
			q.qvs.Set("parse_delay", "0")
			us, err := newUStream(q, nil, "tcp:finite:1234", "finite", 1, uStreamOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer us.cancel()
			gate := make(chan struct{})
			var release sync.Once
			source := &finiteQueueSource{gate: gate, started: make(chan struct{}), terminal: make(chan struct{}), data: tc.wire, finalError: tc.terminal, withData: tc.withData}
			var track byte
			var connections uint32
			source.onClose = func() { track = us.qp.cdTrack; connections = us.serverCount }
			us.io = source
			us.updateUStreamInfoAt = time.Now().Add(time.Hour)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- us.mainLoopIteration(ctx) }()
			stopped := false
			defer func() {
				cancel()
				release.Do(func() { close(gate) })
				if !stopped {
					for i := 0; i < 100; i++ {
						q.tick.Broadcast()
						select {
						case <-done:
							return
						case <-time.After(time.Millisecond):
						}
					}
					t.Error("finite source worker did not stop")
				}
			}()
			select {
			case <-source.started:
			case <-time.After(time.Second):
				t.Fatal("source read did not start")
			}
			for i := 0; i < 3; i++ {
				q.tick.Broadcast()
				time.Sleep(time.Millisecond)
			}
			release.Do(func() { close(gate) })
			select {
			case <-source.terminal:
			case <-time.After(time.Second):
				t.Fatal("terminal input did not arrive")
			}
			// Wait until the reader has committed its bytes and published the error,
			// so the next consumer tick must see terminal IO before parsing data.
			deadline := time.Now().Add(time.Second)
			for len(us.ioECh) == 0 {
				if time.Now().After(deadline) {
					t.Fatal("terminal error was not published")
				}
				time.Sleep(time.Millisecond)
			}
			for i := 0; i < 200; i++ {
				q.tick.Broadcast()
				select {
				case err := <-done:
					stopped = true
					if err == nil || track != tc.wantTrack || (connections != 0) != tc.wantConnection {
						t.Fatalf("terminal input lost accepted state: err=%v track=%d connections=%d", err, track, connections)
					}
					return
				case <-time.After(time.Millisecond):
				}
			}
			t.Fatal("terminal source did not finish")
		})
	}
}

func TestUpstreamQueueSettingsAndReset(t *testing.T) {
	q := queueTestQTV()
	tcp, e := newUStream(q, nil, "tcp:localhost:1234", "test", 1, uStreamOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer tcp.cancel()
	file, e := newUStream(q, nil, "file:test.mvd", "file", 2, uStreamOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer file.cancel()
	if tcp.rb.Capacity() != 4*1024*1024 || file.rb.Capacity() != localMVDReadAheadLimit || len(tcp.rb.blocks) != 0 {
		t.Fatal("default budgets/lazy construction")
	}
	q.qvs.Set("ustream_read_buf_size", "64")
	small, e := newUStream(q, nil, "file:test.mvd", "file", 3, uStreamOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer small.cancel()
	if small.rb.Capacity() != 64 {
		t.Fatal("file configured cap")
	}
	tcp.rb.WriteFull([]byte("old"))
	notifyQueue(tcp.readReady)
	tcp.mvdTime.valid = true
	tcp.mvdScratch = make([]byte, 123)
	if e := tcp.setState(usInitial); e != nil {
		t.Fatal(e)
	}
	if tcp.rb.Capacity() != 4*1024*1024 || tcp.rb.Length() != 0 || len(tcp.mvdScratch) != 0 || tcp.mvdTime.valid || len(tcp.rb.dataReady) != 0 || len(tcp.readReady) != 0 {
		t.Fatal("reconnect changed budget or retained data")
	}
}
func TestUpstreamQueueHeaderAcrossBlocks(t *testing.T) {
	for block := 1; block <= 12; block++ {
		q := queueTestQTV()
		us, e := newUStream(q, nil, "tcp:localhost:1234", "test", 1, uStreamOptions{})
		if e != nil {
			t.Fatal(e)
		}
		us.rb = mustByteQueue(100, block)
		us.setState(usParsingHeader)
		header := []byte("QTVSV 1\nBEGIN\n\n")
		us.rb.WriteFull(append(header, 0, 6, 0))
		if e := us.parseHeader(); e != nil {
			t.Fatal(block, e)
		}
		if us.getState() != usParsingConnection || !bytes.Equal(queueBytes(us.rb), []byte{0, 6, 0}) {
			t.Fatal("header discarded binary suffix")
		}
		us.cancel()
	}
	q := queueTestQTV()
	us, e := newUStream(q, nil, "tcp:localhost:1234", "test", 1, uStreamOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer us.cancel()
	us.rb = mustByteQueue(5, 2)
	us.setState(usParsingHeader)
	us.rb.WriteFull([]byte("QTVSV"))
	if us.parseHeader() == nil {
		t.Fatal("full header deadlock")
	}
}
func TestUpstreamQueueMVDSpansAndBudget(t *testing.T) {
	for block := 1; block < 17; block++ {
		us, viewer := bufferingTestStream(t, 0, false)
		us.rb = mustByteQueue(100, block)
		data := bufferingTestRecord(6, []byte{32, 9})
		data = append(data, bufferingTestRecord(6, []byte{32, 7})...)
		for _, b := range data {
			us.rb.WriteFull([]byte{b})
		}
		if e := us.parseMVD(); e != nil {
			t.Fatal(block, e)
		}
		if !bytes.Equal(queueBytes(viewer.wb), data) || us.rb.Length() != 0 {
			t.Fatal("span changed records")
		}
	}
	us, _ := bufferingTestStream(t, 0, false)
	us.rb = mustByteQueue(10, 3)
	header := []byte{0, 6}
	header = binary.LittleEndian.AppendUint32(header, 20)
	us.rb.WriteFull(header)
	if us.parseMVD() == nil {
		t.Fatal("advertised record larger than queue not rejected")
	}
	us, _ = bufferingTestStream(t, 0, false)
	us.rb = mustByteQueue(10, 3)
	us.rb.WriteFull([]byte{0, 3, 0, 0, 0, 0, 0, 0, 0, 0})
	if us.parseMVD() == nil {
		t.Fatal("full hidden lookahead deadlock")
	}
}
func TestUpstreamQueueFilePrefetch(t *testing.T) {
	q := queueTestQTV()
	dir := t.TempDir()
	data := bytes.Repeat(bufferingTestRecord(6, []byte{32, 7}), 100000)
	if e := os.WriteFile(filepath.Join(dir, "prefetch.mvd"), data, 0600); e != nil {
		t.Fatal(e)
	}
	us, e := newUStream(q, nil, "file:prefetch.mvd", "file", 1, uStreamOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer us.cancel()
	f, e := os.Open(filepath.Join(dir, "prefetch.mvd"))
	if e != nil {
		t.Fatal(e)
	}
	us.io.(*uStreamFile).file = f
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := us.rb.ReadFromWithContext(ctx, us.io); done <- e }()
	deadline := time.Now().Add(time.Second)
	for us.rb.Length() < localMVDReadAheadLimit && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if us.rb.Length() != localMVDReadAheadLimit {
		t.Fatal("file prefetch budget", us.rb.Length())
	}
	cur := us.rb.Snapshot()
	c := cur.cursor(0)
	record, status := inspectMVDRecord(&c, false)
	cur.release()
	if status != mvdRecordComplete {
		t.Fatal("prefetch framing")
	}
	us.rb.Discard(record.size)
	deadline = time.Now().Add(time.Second)
	for us.rb.Length() < localMVDReadAheadLimit && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if us.rb.Length() != localMVDReadAheadLimit {
		t.Fatal("file did not resume after discard")
	}
	cancel()
	us.io.Close()
	select {
	case e := <-done:
		if e != nil && e != context.Canceled && e != io.EOF {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("file worker leaked")
	}
	us.rb.Reset()
}

// Short TCP fragments are grouped by the source consumer instead of waking the
// reader for every packet. A short read is not EOF and must retain its data.
func TestUpstreamQueueGroupsShortTCPReads(t *testing.T) {
	us, err := newUStream(queueTestQTV(), nil, "tcp:localhost:1234", "test", 1, uStreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer us.cancel()
	a, b := net.Pipe()
	us.io.(*uStreamTCP).conn = a
	us.ioECh = make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- us.ioReader(ctx) }()
	defer func() {
		cancel()
		a.Close()
		b.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("reader ignored cancellation")
		}
	}()
	if _, err := b.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	b.SetWriteDeadline(time.Now().Add(40 * time.Millisecond))
	if n, err := b.Write([]byte("cd")); n != 0 || err == nil {
		t.Fatalf("short reads were not grouped: second fragment accepted, n=%d err=%v", n, err)
	}
	if got := queueBytes(us.rb); string(got) != "ab" {
		t.Fatalf("accepted input changed: %q", got)
	}
	b.SetWriteDeadline(time.Now().Add(time.Second))
	notifyQueue(us.readReady)
	if _, err := b.Write([]byte("cd")); err != nil {
		t.Fatal("reader did not resume on the consumer tick", err)
	}
}
