package qtv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func TestDownstreamQueueCommandSpans(t *testing.T) {
	for block := 1; block <= 5; block++ {
		ds := &dStream{state: dsActive, rb: mustByteQueue(32, block), wb: mustByteQueue(128, 16)}
		wire := []byte{4, 0, 1, 0, 3, 0, 99}
		ds.rb.WriteFull(wire[:1])
		done, e := ds.processClientInputMsg()
		if !done || e != nil {
			t.Fatal(e)
		}
		ds.rb.WriteFull(wire[1:])
		if e := ds.processClientInput(); e != nil || ds.rb.Length() != 0 {
			t.Fatal(block, e)
		}
	}
	for _, length := range []uint16{0, 1, 2, 33, 65535} {
		ds := &dStream{state: dsActive, rb: mustByteQueue(32, 4)}
		ds.rb.WriteFull(binary.LittleEndian.AppendUint16(nil, length))
		if _, e := ds.processClientInputMsg(); e == nil {
			t.Fatal(length)
		}
	}
}
func TestDownstreamQueueFullHeader(t *testing.T) {
	ds := &dStream{state: dsParsingHeader, rb: mustByteQueue(8, 3), headers: map[string]string{}}
	ds.rb.WriteFull([]byte("VERSION:"))
	if e := ds.parseHeader(); e == nil {
		t.Fatal("full line waits forever")
	}
}

// Bursts crossing 32 KiB must reuse the viewer's third block after warmup,
// while larger peaks remain bounded and Reset releases the cached payload.
func TestDownstreamQueueReusesThreeBlockBursts(t *testing.T) {
	q := queueTestQTV()
	(&dStreamStorage{}).regVars(q)
	ctx, cancel := context.WithCancel(q.ctx)
	cancel()
	q.ctx = ctx
	a, b := net.Pipe()
	defer b.Close()
	notify := make(chan dStreamId, 1)
	ds, err := newDStream(q, notify, a, 1)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("canceled viewer IO did not stop")
	}
	deadline := time.Now().Add(time.Second)
	for !ds.isCanceled() {
		if time.Now().After(deadline) {
			t.Fatal("viewer cleanup did not finish")
		}
		time.Sleep(time.Millisecond)
	}

	if ds.wb.blockSize != 16*1024 || ds.wb.Capacity() != 192*1024 || len(ds.wb.blocks) != 0 || len(ds.wb.spares) != 0 {
		t.Fatal("viewer output must keep its lazy 16 KiB blocks and byte budget")
	}
	payload := make([]byte, 32*1024+1)
	for i := range payload {
		payload[i] = byte(i / 1024)
	}
	cycle := func() {
		if _, err := ds.wb.WriteFull(payload); err != nil {
			t.Fatal(err)
		}
		view := ds.wb.Snapshot()
		offset := 0
		for _, part := range view.parts {
			if !bytes.Equal(part, payload[offset:offset+len(part)]) {
				t.Fatal("reused viewer blocks changed payload")
			}
			offset += len(part)
		}
		if offset != len(payload) {
			t.Fatal("reused viewer blocks truncated payload")
		}
		view.release()
		if err := ds.wb.Discard(len(payload)); err != nil {
			t.Fatal(err)
		}
	}
	cycle()
	if n := testing.AllocsPerRun(100, cycle); n != 0 {
		t.Fatalf("three-block viewer burst allocations: %g", n)
	}
	peak := make([]byte, ds.wb.Capacity())
	if _, err := ds.wb.WriteFull(peak); err != nil {
		t.Fatal(err)
	}
	if err := ds.wb.Discard(len(peak)); err != nil {
		t.Fatal(err)
	}
	if len(ds.wb.blocks) != 0 || len(ds.wb.spares) != 3 {
		t.Fatal("viewer retained peak output storage", len(ds.wb.spares))
	}
	ds.wb.Reset()
	if ds.wb.Length() != 0 || len(ds.wb.blocks) != 0 || len(ds.wb.spares) != 0 {
		t.Fatal("reset retained viewer payload")
	}
	cycle()
	if n := testing.AllocsPerRun(100, cycle); n != 0 {
		t.Fatalf("reset lost the viewer reuse budget: %g allocations", n)
	}
	if _, err := ds.rb.WriteFull(make([]byte, ds.rb.Capacity())); err != nil {
		t.Fatal(err)
	}
	if err := ds.rb.Discard(ds.rb.Length()); err != nil {
		t.Fatal(err)
	}
	if len(ds.rb.spares) != 2 {
		t.Fatal("viewer input reuse budget changed", len(ds.rb.spares))
	}
}

func TestDownstreamQueueReplyFlush(t *testing.T) {
	q := queueTestQTV()
	q.uss = &uStreamStorage{qtv: q}
	(&dStreamStorage{}).regVars(q)
	q.tick = sync.NewCond(&sync.Mutex{})
	f, e := os.CreateTemp(t.TempDir(), "demo-*.mvd")
	if e != nil {
		t.Fatal(e)
	}
	info, e := f.Stat()
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	demos := make([]DemoListItem, 20000)
	for i := range demos {
		demos[i].FileInfo = info
	}
	q.setDemoList(demos)
	ctx, cancel := context.WithCancel(context.Background())
	q.ctx = ctx
	defer cancel()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				q.tick.Broadcast()
			}
		}
	}()
	a, b := net.Pipe()
	defer b.Close()
	b.SetDeadline(time.Now().Add(3 * time.Second))
	notify := make(chan dStreamId, 1)
	ds, e := newDStream(q, notify, a, 1)
	if e != nil {
		t.Fatal(e)
	}
	if ds.rb.Capacity() != 32768 || ds.wb.Capacity() != 196608 {
		t.Fatal("viewer defaults")
	}
	// Fragment the request across IO reads, using the real main loop and reply builder.
	for _, part := range []string{"QT", "V\nVERSION:", " 1\nDEMOLIST\n", "\n"} {
		if _, e := b.Write([]byte(part)); e != nil {
			t.Fatal(e)
		}
	}
	got, e := io.ReadAll(b)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.HasPrefix(got, []byte("QTVSV 1\n")) || bytes.Count(got, []byte("ADEMO:")) != 20000 || !bytes.HasSuffix(got, []byte("\n\n")) {
		t.Fatal("incomplete flush", len(got))
	}
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("viewer IO leaked")
	}
	if ds.wb.Length() != 0 {
		t.Fatal("exit before queued bytes sent")
	}
}

type queueDeadlineConn struct {
	net.Conn
	ready     chan struct{}
	mu        sync.Mutex
	deadlines []time.Time
}

func (c *queueDeadlineConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	first := len(c.deadlines) == 0
	c.deadlines = append(c.deadlines, t)
	c.mu.Unlock()
	if first {
		t = time.Now().Add(200 * time.Millisecond)
	}
	e := c.Conn.SetDeadline(t)
	if first {
		close(c.ready)
	}
	return e
}
func TestDownstreamQueueProgressAndSilentTimeout(t *testing.T) {
	for _, progress := range []bool{false, true} {
		t.Run(fmt.Sprint(progress), func(t *testing.T) {
			q := queueTestQTV()
			q.qvs.Reg("dstream_timeout", "1")
			q.tick = sync.NewCond(&sync.Mutex{})
			a, b := net.Pipe()
			defer b.Close()
			b.SetReadDeadline(time.Now().Add(2 * time.Second))
			conn := &queueDeadlineConn{Conn: a, ready: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-stop:
						return
					case <-tick.C:
						q.tick.Broadcast()
					}
				}
			}()
			us := &uStream{dsCloseNotifyCh: make(chan dStreamId, 1)}
			ds := &dStream{qtv: q, conn: conn, rb: mustByteQueue(32768, 4096), wb: mustByteQueue(196608, 16384), linkedUs: us, ioECh: make(chan error, 2), dssNotifyCh: make(chan dStreamId, 1)}
			done := make(chan error, 1)
			go func() { done <- ds.mainLoop(ctx) }()
			<-conn.ready
			if progress {
				payload := bytes.Repeat([]byte{'x'}, 120000)
				ds.wb.WriteFull(payload)
				got := make([]byte, len(payload))
				if _, e := io.ReadFull(b, got); e != nil {
					t.Fatal(e)
				}
				if !bytes.Equal(got, payload) {
					t.Fatal("progress payload")
				}
				deadline := time.Now().Add(time.Second)
				renewed := false
				for time.Now().Before(deadline) {
					conn.mu.Lock()
					renewed = len(conn.deadlines) > 1
					conn.mu.Unlock()
					if renewed {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if !renewed {
					t.Fatal("positive writes did not renew deadline")
				}
				cancel()
				a.Close()
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("silent peer/closed writer leaked")
			}
		})
	}
}

func TestDownstreamQueueMVDRecord(t *testing.T) {
	for _, tc := range []struct {
		name         string
		capacity     int
		invalid      bool
		checkAvail   int
		wantErr      error
		wantRejected bool
		want         []byte
	}{
		{name: "payload does not fit", capacity: 9, wantErr: errByteQueueFull, wantRejected: true},
		{name: "encoder error", capacity: 10, invalid: true, wantRejected: true},
		{name: "reserved space", capacity: 10, checkAvail: 1, wantErr: errCheckAvail, wantRejected: true},
		{name: "exact fit", capacity: 10, want: []byte{0, 1, 2, 0, 0, 0, 'a', 'b'}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := []byte{0x11, 0x22}
			queue := mustByteQueue(tc.capacity, 4)
			if _, err := queue.WriteFull(prefix); err != nil {
				t.Fatal(err)
			}
			select {
			case <-queue.dataReady:
			default:
				t.Fatal("queue did not signal the existing prefix")
			}
			ds := &dStream{
				state:    dsActive,
				linkedUs: &uStream{mvdHdr: newNetMsgW(make([]byte, 16), false)},
				wb:       queue,
			}
			msg := newNetMsgW(make([]byte, 2), false)
			msg.PutByte('a')
			msg.PutByte('b')
			if tc.invalid {
				msg.PutByte('c')
			}

			err := ds.sendMVDMessageEx(msg, mvdMsgRead, playersMaskAll, tc.checkAvail, 0)
			if (err != nil) != tc.wantRejected || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
				t.Fatalf("unexpected send error: %v", err)
			}
			if got, want := queueBytes(queue), append(prefix, tc.want...); !bytes.Equal(got, want) {
				t.Fatalf("queued bytes = % x, want % x", got, want)
			}
			if notified := len(queue.dataReady) != 0; notified == tc.wantRejected {
				t.Fatalf("queue notification = %t, want %t", notified, !tc.wantRejected)
			}
		})
	}
}

func TestDownstreamQueueDownload(t *testing.T) {
	q := soundTestProtocol()
	us := &uStream{qp: q, mvdHdr: newNetMsgW(make([]byte, 16), false)}
	q.us = us
	ds := &dStream{state: dsActive, linkedUs: us}
	ds.wb = mustByteQueue(32768, 4096)
	data := bytes.Repeat([]byte("download"), 10000)
	f, e := os.CreateTemp(t.TempDir(), "download")
	if e != nil {
		t.Fatal(e)
	}
	if _, e := f.Write(data); e != nil {
		t.Fatal(e)
	}
	f.Seek(0, 0)
	ds.download = dStreamDownload{file: f, size: int64(len(data))}
	defer ds.download.Reset()
	var got []byte
	for i := 0; i < 100 && ds.download.IsActive(); i++ {
		if e := ds.nextDownload(); e != nil {
			t.Fatal(e)
		}
		raw := queueBytes(ds.wb)
		ds.wb.Discard(len(raw))
		for len(raw) > 0 {
			size := int(binary.LittleEndian.Uint32(raw[2:6]))
			payload := raw[6 : 6+size]
			if payload[0] != 41 {
				t.Fatal("not download", payload[0])
			}
			n := int(binary.LittleEndian.Uint16(payload[1:3]))
			got = append(got, payload[4:4+n]...)
			raw = raw[6+size:]
		}
	}
	if ds.download.IsActive() || !bytes.Equal(got, data) {
		t.Fatal("download changed", len(got))
	}
}
func TestDownstreamQueueLargeBudgetDownload(t *testing.T) {
	for _, limit := range []int{192 * 1024, 1 << 30, math.MaxInt32} {
		q := soundTestProtocol()
		us := &uStream{qp: q, mvdHdr: newNetMsgW(make([]byte, 16), false)}
		q.us = us
		f, err := os.CreateTemp(t.TempDir(), "download")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err = f.WriteString("one"); err != nil {
			t.Fatal(err)
		}
		if _, err = f.Seek(0, 0); err != nil {
			t.Fatal(err)
		}
		ds := &dStream{state: dsActive, linkedUs: us, wb: mustByteQueue(limit, 16*1024), download: dStreamDownload{file: f, size: 3}}
		if err := ds.nextDownload(); err != nil {
			t.Fatal(limit, err)
		}
		want := []byte{0, 6, 7, 0, 0, 0, 41, 3, 0, 100, 'o', 'n', 'e'}
		if ds.download.IsActive() || !bytes.Equal(queueBytes(ds.wb), want) {
			t.Fatal("empty large queue stalled or changed download", limit, queueBytes(ds.wb))
		}
	}
}

func TestDownstreamQueueCommandBounded(t *testing.T) {
	ds := &dStream{state: dsActive, rb: mustByteQueue(32, 2)}
	ds.rb.WriteFull([]byte{4, 0, 1, 'x', 3, 0, 0})
	if _, e := ds.processClientInputMsg(); e == nil || ds.rb.Length() != 7 {
		t.Fatal("command used next record as string terminator")
	}
}

// denseQueueTestStream creates the same portable large world for startup bounds
// and viewer-failure isolation, without depending on local capture fixtures.
func denseQueueTestStream() *uStream {
	qtv := queueTestQTV()
	us := &uStream{qtv: qtv, state: usActive, serverCount: 7, mvdHdr: newNetMsgW(make([]byte, 16), false)}
	us.qp = newQProtocol(us)
	qp := us.qp
	qp.extFlagsFTE2 = ftePext2ReplacementDeltas
	qp.gameDir = "qw"
	qp.modelList[1] = "maps/povdmm4.bsp"
	qp.ensureRD().apply(rdFrame{reset: true})
	qp.rd.ensureStorage(maxRDEntities)
	for id := 1; id < maxRDEntities; id++ {
		s := defaultRDState()
		s.number = uint32(id)
		s.modelIndex = 1
		s.origin = [3]float32{float32(id % 1024), 2, 3}
		s.effects = 0xffffffff
		s.frame = 500
		s.light = [4]uint16{1, 2, 3, 4}
		qp.rd.current[id] = s
		qp.rd.present[id] = true
	}
	return us
}

func TestDownstreamQueueDenseSnapshot(t *testing.T) {
	us := denseQueueTestStream()
	qtv := us.qtv
	for _, ext := range []uint32{0, qtvEzQuakeExtDownload} {
		for _, limit := range []int{196608, 2 * 1024 * 1024} {
			ds := &dStream{state: dsNeedInitialData, linkedUs: us, wb: mustByteQueue(limit, 16384), qtvEzQuakeExt: ext}
			e := us.sendInitialMVDData(ds)
			if e == nil && ext != 0 {
				for _, cmd := range []string{"qtvsoundlist 7", "qtvmodellist 7", "qtvspawn 7"} {
					if e = ds.executeClientCommand(cmd); e != nil {
						break
					}
				}
			}
			if limit == 196608 {
				if e == nil || ds.state == dsActive {
					t.Fatal("dense snapshot exceeded default queue", ext)
				}
				continue
			}
			if e != nil || ds.state != dsActive {
				t.Fatal("configured dense queue", ext, e)
			}
			raw := queueBytes(ds.wb)
			if ext == 0 {
				checkDenseFilePrefetch(t, qtv, raw)
			}
			target := &uStream{qtv: qtv, state: usActive}
			target.qp = newQProtocol(target)
			for len(raw) > 0 {
				n := int(binary.LittleEndian.Uint32(raw[2:6]))
				if n > maxMvdSize || len(raw) < 6+n {
					t.Fatal("snapshot framing")
				}
				if e := target.qp.readMessage(raw[6:6+n], mvdMsgType(raw[1]&7), playersMaskAll); e != nil {
					t.Fatal(e)
				}
				raw = raw[6+n:]
			}
			if target.qp.rd == nil || !target.qp.rd.present[maxRDEntities-1] {
				t.Fatal("dense snapshot lost high tail")
			}
		}
	}
}

func TestDownstreamQueueViewerIsolation(t *testing.T) {
	us := denseQueueTestStream()
	initial := &dStream{state: dsNeedInitialData, linkedUs: us, wb: mustByteQueue(192*1024, 16*1024)}
	full := &dStream{state: dsActive, linkedUs: us, wb: mustByteQueue(32, 16)}
	if _, err := full.wb.WriteFull(make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	healthy := &dStream{state: dsActive, linkedUs: us, wb: mustByteQueue(1024, 16)}
	us.linkedDs = map[dStreamId]*dStream{1: initial, 2: full, 3: healthy}
	var want []byte
	for _, record := range [][]byte{{0, 6, 1, 0, 0, 0, 1}, {0, 6, 2, 0, 0, 0, 32, 9}} {
		if err := us.sendMVDDataDown(record); err != nil {
			t.Fatal(err)
		}
		want = append(want, record...)
		if !initial.isCanceled() || !full.isCanceled() || healthy.isCanceled() || us.getState() != usActive {
			t.Fatal("viewer failure affected another connection")
		}
		if !bytes.Equal(queueBytes(healthy.wb), want) {
			t.Fatal("healthy viewer lost or changed stream data")
		}
	}
}

// checkDenseFilePrefetch parses a complete startup larger than the file budget.
func checkDenseFilePrefetch(t *testing.T, qtv *QTV, raw []byte) {
	t.Helper()
	if len(raw) <= localMVDReadAheadLimit {
		t.Fatal("fixture must exceed file prefetch")
	}
	f, err := os.CreateTemp(t.TempDir(), "dense-*.mvd")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(raw); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if _, err = f.Seek(0, 0); err != nil {
		f.Close()
		t.Fatal(err)
	}
	us := &uStream{qtv: qtv, state: usActive, io: &uStreamFile{file: f}, rb: mustByteQueue(localMVDReadAheadLimit, 64*1024)}
	us.qp = newQProtocol(us)
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	var readErr error
	go func() {
		_, readErr = us.rb.ReadFromWithContext(ctx, us.io)
		close(finished)
	}()
	defer func() {
		cancel()
		f.Close()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("file reader did not stop")
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		us.curTime = curTime()
		if err := us.parseMVD(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-finished:
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				t.Fatal(readErr)
			}
			if us.rb.IsEmpty() {
				if us.qp.rd == nil {
					t.Fatal("file startup omitted RD state")
				}
				count := 0
				for _, present := range us.qp.rd.present {
					if present {
						count++
					}
				}
				if count != maxRDEntities-1 || !us.qp.rd.present[maxRDEntities-1] {
					t.Fatal("file startup lost entities", count)
				}
				return
			}
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("file parser stopped making progress")
		}
		time.Sleep(time.Millisecond)
	}
}
