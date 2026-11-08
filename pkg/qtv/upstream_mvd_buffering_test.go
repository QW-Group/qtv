package qtv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/qqshka/ringbuffer"
	"testing"
)

// bufferingTestStream uses the real parser, stream queues and forwarding path.
func bufferingTestStream(t *testing.T, delay int, rd bool) (*uStream, *dStream) {
	t.Helper()
	q := &QTV{}
	q.cmd = newQCmd(q)
	q.qvs = newQVarStorage(q)
	q.qvs.Reg("parse_delay", fmt.Sprint(delay))
	q.qvs.Reg("hostname", "buffering test")
	us := &uStream{qtv: q, io: &uStreamTCP{}, state: usActive, curTime: 1000, parseTime: 1000, rb: mustByteQueue(4096, 64*1024)}
	us.qp = newQProtocol(us)
	if rd {
		us.qp.extFlagsFTE2 = ftePext2ReplacementDeltas
	}
	us.qp.extFlagsMVD1 = mvdPext1HiddenMessages
	viewer := &dStream{id: 1, state: dsActive, wb: ringbuffer.New(4096)}
	us.linkedDs = map[dStreamId]*dStream{1: viewer}
	return us, viewer
}

// bufferingTestRecord frames literal service bytes as a real MVD record.
func bufferingTestRecord(kind byte, payload []byte) []byte {
	b := []byte{0, kind}
	if kind == 3 {
		b = binary.LittleEndian.AppendUint32(b, ^uint32(0))
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, payload...)
}

func TestMVDProgressPreservesPlaybackClock(t *testing.T) {
	good := bufferingTestRecord(6, []byte{32, 7}) // svc_cdtrack marker.
	records := map[string][]byte{
		"read":   bufferingTestRecord(1, []byte{32, 9}),
		"all":    bufferingTestRecord(6, []byte{32, 9}),
		"single": bufferingTestRecord(4, []byte{32, 9}),
		"stats":  bufferingTestRecord(5, []byte{32, 9}),
		"multi":  bufferingTestRecord(3, []byte{32, 9}),
		"set":    {0, 2, 0, 0, 0, 0, 0, 0, 0, 0},
		"hidden": {0, 3, 0, 0, 0, 0, 7, 0, 0, 0, 3, 0, 0, 0, 10, 0, 0},
	}
	for _, delay := range []int{7, 15} {
		for _, rd := range []bool{false, true} {
			for name, record := range records {
				for cut := 0; cut < len(record); cut++ {
					t.Run(fmt.Sprintf("delay%d/rd%v/%s/cut%d", delay, rd, name, cut), func(t *testing.T) {
						us, viewer := bufferingTestStream(t, delay, rd)
						input := append(append([]byte{}, good...), record[:cut]...)
						if _, err := us.rb.WriteFull(input); err != nil {
							t.Fatal(err)
						}
						if err := us.parseMVD(); err != nil {
							t.Fatal(err)
						}
						if us.parseTime != 1000 || us.qp.cdTrack != 7 || !bytes.Equal(queueBytes(us.rb), record[:cut]) || !bytes.Equal(viewer.wb.BytesOneReader(), good) {
							t.Fatalf("progress changed clock or consumed partial record: clock=%d pending=%x forwarded=%x", us.parseTime, queueBytes(us.rb), viewer.wb.BytesOneReader())
						}
						if _, err := us.rb.WriteFull(record[cut:]); err != nil {
							t.Fatal(err)
						}
						us.curTime = 1100 // Ordinary next tick, not two seconds later.
						if err := us.parseMVD(); err != nil {
							t.Fatal(err)
						}
						want := append(append([]byte{}, good...), record...)
						if us.rb.Length() != 0 || !bytes.Equal(viewer.wb.BytesOneReader(), want) || us.parseTime != 1000 {
							t.Fatal("completed record did not pass on the next tick")
						}
					})
				}
			}
		}
	}
}

func TestMVDNoProgressKeepsRebuffering(t *testing.T) {
	for _, delay := range []int{7, 15} {
		for _, partial := range [][]byte{nil, {0}, {0, 6, 2, 0, 0, 0, 32}} {
			us, viewer := bufferingTestStream(t, delay, false)
			us.rb.WriteFull(partial)
			if err := us.parseMVD(); err != nil {
				t.Fatal(err)
			}
			if us.parseTime != 3000 || !bytes.Equal(queueBytes(us.rb), partial) || viewer.wb.Length() != 0 {
				t.Fatal("no-progress buffering or partial retention changed")
			}
		}
	}
}

func TestMVDProgressHonorsFutureDeadline(t *testing.T) {
	for _, delay := range []int{7, 15} {
		us, viewer := bufferingTestStream(t, delay, false)
		us.parseTime = 2000
		good := bufferingTestRecord(6, []byte{32, 7})
		us.rb.WriteFull(good)
		if err := us.parseMVD(); err != nil {
			t.Fatal(err)
		}
		if us.parseTime != 2000 || us.rb.Length() != len(good) || viewer.wb.Length() != 0 {
			t.Fatal("future playback deadline was bypassed")
		}
		us.curTime = 2000
		if err := us.parseMVD(); err != nil {
			t.Fatal(err)
		}
		if us.qp.cdTrack != 7 || !bytes.Equal(viewer.wb.BytesOneReader(), good) {
			t.Fatal("due record was not forwarded")
		}
	}
}

func TestMVDNoProgressRecoveryWaitsForDeadline(t *testing.T) {
	us, viewer := bufferingTestStream(t, 15, false)
	if err := us.parseMVD(); err != nil {
		t.Fatal(err)
	}
	good := bufferingTestRecord(6, []byte{32, 9})
	us.rb.WriteFull(good)
	us.curTime = 1100
	if err := us.parseMVD(); err != nil {
		t.Fatal(err)
	}
	if us.parseTime != 3000 || viewer.wb.Length() != 0 || us.rb.Length() != len(good) {
		t.Fatal("arriving data bypassed the existing underrun deadline")
	}
	us.curTime = 3000
	if err := us.parseMVD(); err != nil {
		t.Fatal(err)
	}
	if us.rb.Length() != 0 || !bytes.Equal(viewer.wb.BytesOneReader(), good) || us.parseTime != 3000 {
		t.Fatal("recovery failed or added another pause after progress")
	}
}

func TestMVDProgressAdvancesRecordTime(t *testing.T) {
	us, viewer := bufferingTestStream(t, 7, false)
	first := bufferingTestRecord(6, []byte{32, 7})
	first[0] = 20
	second := bufferingTestRecord(6, []byte{32, 9})
	us.rb.WriteFull(append(append([]byte{}, first...), second...))
	// Twenty buffered milliseconds / seven-second target clamps speed to 0.1.
	// The first record therefore advances the playback clock by 200 ms.
	if err := us.parseMVD(); err != nil {
		t.Fatal(err)
	}
	if us.parseTime != 1200 || us.qp.cdTrack != 7 || us.rb.Length() != len(second) {
		t.Fatal("record time no longer gates the next record")
	}
	us.curTime = 1100
	if err := us.parseMVD(); err != nil || us.qp.cdTrack != 7 {
		t.Fatal("next tick bypassed the record timestamp", err)
	}
	us.curTime = 1200
	if err := us.parseMVD(); err != nil || us.qp.cdTrack != 9 || us.parseTime != 1200 {
		t.Fatal("due record failed or rebuffered after progress", err)
	}
	want := append(append([]byte{}, first...), second...)
	want[0] = 200 // Forwarded timestamps retain the existing speed normalization.
	if !bytes.Equal(viewer.wb.BytesOneReader(), want) {
		t.Fatal("forwarded packet timing or order changed")
	}
}

func TestMVDFileDoesNotRebuffer(t *testing.T) {
	us, viewer := bufferingTestStream(t, 15, false)
	us.io = &uStreamFile{}
	if err := us.parseMVD(); err != nil || us.parseTime != 1000 {
		t.Fatal("empty file source scheduled live rebuffering", err)
	}
	good := bufferingTestRecord(6, []byte{32, 7})
	us.rb.WriteFull(append(append([]byte{}, good...), 0))
	if err := us.parseMVD(); err != nil || us.parseTime != 1000 {
		t.Fatal("file progress changed the playback clock", err)
	}
	if !bytes.Equal(queueBytes(us.rb), []byte{0}) || !bytes.Equal(viewer.wb.BytesOneReader(), good) {
		t.Fatal("file partial tail or completed record changed")
	}
}

func TestMVDEOFDrainsProgressBeforeError(t *testing.T) {
	us, viewer := bufferingTestStream(t, 7, false)
	us.ioError = true
	good := bufferingTestRecord(6, []byte{32, 7})
	us.rb.WriteFull(append(append([]byte{}, good...), 0))
	if err := us.parseMVD(); err != nil {
		t.Fatal("EOF prevented forwarding complete records", err)
	}
	if !bytes.Equal(viewer.wb.BytesOneReader(), good) || us.rb.Length() != 1 {
		t.Fatal("EOF lost completed records or the partial tail")
	}
	us.curTime = 1100
	if err := us.parseMVD(); err == nil {
		t.Fatal("EOF without another complete record was ignored")
	}
}
