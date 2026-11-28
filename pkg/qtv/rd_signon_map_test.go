package qtv

import (
	"bytes"
	"errors"
	"testing"
)

func TestRDSignonBaselineZeroKeepsWorld(t *testing.T) {
	us := denseQueueTestStream()
	qp := us.qp
	ds := rdTestViewer(qp, 0)
	ds.wb = mustByteQueue(192*1024, 16*1024)
	if err := us.startRDSignon(ds); err != nil || !ds.rdSignon.attached {
		t.Fatalf("expected paged signon: %v", err)
	}
	base := defaultRDState()
	base.modelIndex = 1
	msg := qp.w.Clear()
	qp.putRDClassicSignon(msg, base, true)
	if err := qp.readMessage(msg.Bytes(), mvdMsgAll, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	got, _ := rdTestDecodeSnapshot(t, drainRDSignon(t, ds))
	if !got.rd.baselinePresent[0] || got.rd.baselines[0].modelIndex != 1 ||
		!got.rd.present[2] || !got.rd.present[maxRDEntities-1] {
		t.Fatal("refreshing baseline zero reset live entities")
	}
}

func TestRDSignonWaitsForCorrectionSpace(t *testing.T) {
	us := denseQueueTestStream()
	ds := rdTestViewer(us.qp, 0)
	ds.wb = mustByteQueue(32*1024, 4096)
	if err := us.startRDSignon(ds); err != nil || !ds.rdSignon.attached {
		t.Fatalf("expected paged signon: %v", err)
	}
	// The final page has been queued. A correction this large will fail as
	// soon as QTV tries to encode it.
	ds.rdSignon.offset = len(us.rdSignon.data)
	for id := 1; id < len(us.qp.rd.current); id++ {
		us.rdSignon.dirty.set(uint32(id))
	}
	if err := ds.wb.Discard(ds.wb.Length()); err != nil {
		t.Fatal(err)
	}
	if _, err := ds.wb.WriteFull(make([]byte, ds.wb.Capacity()-ds.rdSignonHeadroom())); err != nil {
		t.Fatal(err)
	}
	if err := us.pageRDSignon(ds); err != nil || ds.state == dsActive {
		t.Fatalf("encoded correction while only live headroom was free: %v", err)
	}
	if err := ds.wb.Discard(ds.wb.Length()); err != nil {
		t.Fatal(err)
	}
	if err := us.pageRDSignon(ds); !errors.Is(err, errByteQueueFull) {
		t.Fatalf("expected oversized correction after queue drained, got %v", err)
	}
}

func TestRDSignonMapEnd(t *testing.T) {
	for _, tc := range []struct {
		name               string
		floatCoords, paged bool
	}{
		{"fixed combined signon", false, false},
		{"float paged signon", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			us := denseQueueTestStream()
			qp := us.qp
			ds := rdTestViewer(qp, 0)
			if tc.floatCoords {
				qp.extFlagsFTE1 |= ftePextFloatCoords
				qp.w.floatCoord = true
			}
			if tc.paged {
				ds.wb = mustByteQueue(192*1024, 16*1024)
				if err := us.startRDSignon(ds); err != nil || !ds.rdSignon.attached {
					t.Fatalf("expected paged signon: %v", err)
				}
			} else {
				clear(qp.rd.present)
			}
			msg := qp.w.Clear()
			msg.PutSVC(svc_intermission)
			for _, v := range []float32{80, 32, -24} {
				msg.PutCoord(v)
			}
			for _, v := range []float32{45, 90, 135} {
				msg.PutAngle(v)
			}
			camera := bytes.Clone(msg.Bytes())
			finale := []byte{byte(svc_finale), 0}
			apply := func(payload []byte, kind mvdMsgType, mask uint32) {
				t.Helper()
				if err := qp.readMessage(payload, kind, mask); err != nil {
					t.Fatal(err)
				}
			}
			old := []byte{byte(svc_finale), 'x', 0}
			apply(old, mvdMsgAll, playersMaskAll)
			apply(camera, mvdMsgRead, playersMaskAll)
			apply(finale, mvdMsgRead, playersMaskAll)
			private := []byte{byte(svc_finale), 'p', 0}
			apply(private, mvdMsgSingle, 1<<3)
			apply(private, mvdMsgRead, playersMaskAll)
			if !tc.paged {
				if err := us.startRDSignon(ds); err != nil || ds.state != dsActive {
					t.Fatalf("expected direct signon: %v", err)
				}
			}
			wire := drainRDSignon(t, ds)
			want := append(rdTestMVD(camera), rdTestMVD(finale)...)
			at := bytes.Index(wire, want)
			if at < 0 || bytes.Count(wire, want) != 1 || at >= bytes.Index(wire, []byte("skins\n\x00")) ||
				bytes.Contains(wire, rdTestMVD(private)) || bytes.Contains(wire, rdTestMVD(old)) {
				t.Fatal("expected current broadcast camera and empty finale once before skins")
			}
			// The first live dem_read must still refer to player 3.
			tail := wire[len(wire)-6:]
			if tail[1] != byte(mvdMsgSingle)|(3<<3) || tail[0] != 5 {
				t.Fatalf("wrong route at handoff: %x", tail)
			}
			qp.resetReaderData()
			qp.extFlagsFTE2 = ftePext2ReplacementDeltas
			if err := ds.wb.Discard(ds.wb.Length()); err != nil {
				t.Fatal(err)
			}
			if err := ds.sendSpawnEnd(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(queueBytes(ds.wb), rdTestMVD(camera)) || bytes.Contains(queueBytes(ds.wb), rdTestMVD(finale)) {
				t.Fatal("previous map end replayed on next map")
			}
		})
	}
}

func TestRDSignonHiddenRouteAcrossMaps(t *testing.T) {
	qp := denseQueueTestStream().qp
	qp.us.wb = mustByteQueue(4096, 512)
	qp.extFlagsMVD1 = mvdPext1HiddenMessages
	parse := func(record []byte) {
		t.Helper()
		cur := byteViewOf(record).cursor(0)
		h, status := inspectMVDRecord(&cur, mvdContainerTime)
		if status != mvdRecordComplete {
			t.Fatal("invalid test record")
		}
		if _, err := qp.us.parseMVDMessage(h, record, 1); err != nil {
			t.Fatal(err)
		}
	}
	parse([]byte{0, byte(mvdMsgMulti), 0, 0, 0, 0, 0, 0, 0, 0})
	parse([]byte{0, byte(mvdMsgSet), 0, 0, 0, 0, 0, 0, 0, 0})
	// Hidden data beginning with 0xff is not a Quake service message.
	parse([]byte{0, byte(mvdMsgRead), 4, 0, 0, 0, 255, 255, 255, 255})
	ds := rdTestViewer(qp, 0)
	if err := qp.sendPlaybackDelay(ds, 5); err != nil {
		t.Fatal(err)
	}
	if wire := queueBytes(ds.wb); !bytes.Equal(wire, []byte{5, byte(mvdMsgMulti), 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("lost hidden route: %x", wire)
	}
	if err := ds.wb.Discard(ds.wb.Length()); err != nil {
		t.Fatal(err)
	}
	if err := qp.sendServerData(ds); err != nil {
		t.Fatal(err)
	}
	oldMap := qp.us.serverCount
	parse(queueBytes(ds.wb))
	if qp.us.serverCount != oldMap+1 || qp.messageType != mvdMsgAll {
		t.Fatal("new serverdata inherited the previous map's hidden route")
	}
}

func TestRDSignonMapEndReplacesOnlyCompleteMessages(t *testing.T) {
	qp := rdTestProtocol()
	finale := []byte{byte(svc_finale), 'a', 0}
	if err := qp.readMessage(finale, mvdMsgAll, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	camera := make([]byte, 10)
	camera[0] = byte(svc_intermission)
	for _, partial := range [][]byte{camera[:9], finale[:2]} {
		if err := qp.readMessage(partial, mvdMsgAll, playersMaskAll); err == nil {
			t.Fatal("accepted truncated map-end message")
		}
		if qp.intermission != nil || !bytes.Equal(qp.finale, finale) {
			t.Fatal("truncated message replaced the standalone finale")
		}
	}
	if err := qp.readMessage(camera, mvdMsgAll, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(qp.intermission, camera) || qp.finale != nil {
		t.Fatal("new intermission kept the old finale")
	}
}
