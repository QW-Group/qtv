package qtv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// rdWriterSource retains a rich independently encoded world for viewer snapshots.
func rdWriterSource(t *testing.T, floats bool) *qProtocol {
	t.Helper()
	q := rdTestProtocol()
	if floats {
		q.extFlagsFTE1 = ftePextFloatCoords
	}
	q.w.floatCoord = floats
	q.us.state = usActive
	q.us.serverCount = 7
	q.us.mvdHdr = newNetMsgW(make([]byte, 16), false)
	width := 2
	if floats {
		width = 4
	}
	fixture := rdTestPayload(t, width)
	b := append([]byte{66, 255, 63}, fixture...)
	b = append(b, 21)
	b = append(b, fixture...)
	if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	b = append([]byte{86}, rdTestFrame(append([]byte{0, 128, 1, 0}, fixture...))...)
	if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	q.modelList[1] = "maps/povdmm4.bsp"
	for i := 2; i <= 511; i++ {
		q.modelList[i] = fmt.Sprintf("progs/model%d.mdl", i)
	}
	q.soundList[1] = "weapons/rocket1i.wav"
	q.lightStyle[0] = "m"
	if err := q.players[0].userInfo.Set("name", "RDPlayer"); err != nil {
		t.Fatal(err)
	}
	q.players[0].userId = 17
	q.players[0].stats[0] = 100
	if err := q.players[1].userInfo.Set("name", "OtherPlayer"); err != nil {
		t.Fatal(err)
	}
	q.players[1].stats[0] = 37
	q.isPaused = true
	return q
}

// rdTestViewer queues startup bytes without a socket or IO worker.
func rdTestViewer(q *qProtocol, ext uint32) *dStream {
	return &dStream{state: dsNeedInitialData, linkedUs: q.us, wb: mustByteQueue(1024*1024, 16*1024), qtvEzQuakeExt: ext}
}

// rdTestSignon completes either plain startup or the download-capable handshake.
func rdTestSignon(t *testing.T, ds *dStream) {
	t.Helper()
	us := ds.linkedUs
	us.mu.Lock()
	defer us.mu.Unlock()
	if err := us.sendInitialMVDData(ds); err != nil {
		t.Fatal(err)
	}
	if ds.qtvEzQuakeExt != 0 {
		for _, name := range []string{"qtvsoundlist", "qtvmodellist", "qtvspawn"} {
			cmd := fmt.Sprintf("%s %d", name, us.serverCount)
			if err := ds.executeClientCommand(cmd); err != nil {
				t.Fatal(cmd, err)
			}
		}
	}
	if ds.state != dsActive {
		t.Fatal("signon incomplete", ds.state)
	}
}

// rdTestDecodeSnapshot reads startup records and enforces per-message budgets.
func rdTestDecodeSnapshot(t *testing.T, data []byte) (*qProtocol, int) {
	t.Helper()
	q := rdTestProtocol()
	q.extFlagsFTE2 = 0 // The snapshot must advertise its own RD profile.
	chunks := 0
	for len(data) > 0 {
		if len(data) < 2 {
			t.Fatal("short MVD header")
		}
		kind := mvdMsgType(data[1]) & mvdMsgMask
		header, size := 6, 10
		if kind == mvdMsgMulti {
			header = 10
		}
		if kind != mvdMsgSet {
			if len(data) < header {
				t.Fatal("short MVD length header")
			}
			n := int(binary.LittleEndian.Uint32(data[header-4 : header]))
			if n < 0 || n > maxMvdSize || len(data)-header < n {
				t.Fatal("bad MVD size", n)
			}
			size = header + n
			payload := data[header:size]
			if len(payload) > 0 && payload[0] == byte(svc_fte_updateentities) {
				chunks++
				if len(payload) > maxMsgSize {
					t.Fatal("oversize RD chunk")
				}
			}
		} else if len(data) < size {
			t.Fatal("short MVD set record")
		}
		// Parse the real recipient header, including directed player statistics.
		done, used, _, err := q.us.parseMVDMessage(data[:size], 1)
		if err != nil || done || used != size {
			t.Fatalf("snapshot decode: err=%v done=%v used=%d want=%d", err, done, used, size)
		}
		data = data[size:]
	}
	return q, chunks
}

func TestRDLateViewer(t *testing.T) {
	for _, coordinates := range []struct {
		name               string
		floats, entityOnly bool
	}{
		{"fixed", false, false},
		{"float", true, false},
		{"entity-only-float", true, true},
	} {
		for _, ext := range []uint32{0, qtvEzQuakeExtDownload} {
			t.Run(fmt.Sprintf("%s/download=%v", coordinates.name, ext != 0), func(t *testing.T) {
				q := rdWriterSource(t, coordinates.floats)
				if coordinates.entityOnly {
					q.extFlagsFTE1 = 0
					q.extFlagsMVD1 = mvdPext1FloatCoords
					q.w.floatCoord = false
				}
				first := rdTestViewer(q, 0)
				rdTestSignon(t, first)
				saved := append([]byte(nil), queueBytes(first.wb)...)
				ds := rdTestViewer(q, ext)
				rdTestSignon(t, ds)
				decoded, chunks := rdTestDecodeSnapshot(t, queueBytes(ds.wb))
				if chunks == 0 || decoded.rd == nil || !decoded.rd.ready || !decoded.rd.present[1] || decoded.players[0].stats[0] != 100 || decoded.players[1].stats[0] != 37 || decoded.players[2].stats[0] != 0 || decoded.players[0].userInfo.Get("name") != "RDPlayer" || !decoded.isPaused {
					t.Fatal("incomplete late viewer snapshot")
				}
				if decoded.modelList[511] != q.modelList[511] || decoded.soundList[1] != q.soundList[1] {
					t.Fatal("resource catalog lost")
				}
				if !reflect.DeepEqual(q.rd.current[1], decoded.rd.current[1]) || !reflect.DeepEqual(q.rd.baselines[16383], decoded.rd.baselines[16383]) || !reflect.DeepEqual(q.rd.statics, decoded.rd.statics) {
					t.Fatalf("lost raw RD state\n got %+v\nwant %+v", decoded.rd.current[1], q.rd.current[1])
				}
				if !bytes.Equal(saved, queueBytes(first.wb)) {
					t.Fatal("joining viewer disturbed existing viewer")
				}
			})
		}
	}
}

func TestRDSnapshotChunks(t *testing.T) {
	q := rdTestProtocol()
	q.us.state = usActive
	q.us.mvdHdr = newNetMsgW(make([]byte, 16), false)
	q.rd = &rdWorld{}
	if err := q.rd.apply(rdFrame{time: 8, reset: true}); err != nil {
		t.Fatal(err)
	}
	ds := rdTestViewer(q, 0)
	if err := q.sendRDEnts(ds); err != nil {
		t.Fatal(err)
	}
	prefix := rdTestMVD(rdTestServerData(0, ftePext2ReplacementDeltas, 0))
	empty, chunks := rdTestDecodeSnapshot(t, append(prefix, queueBytes(ds.wb)...))
	if chunks != 1 || !empty.rd.ready {
		t.Fatal("empty world must reset")
	}
	for i := 1; i < 700; i++ {
		s := defaultRDState()
		s.number = uint32(i)
		s.origin = [3]float32{float32(i), 2, 3}
		s.modelIndex = 7
		s.frame = 300
		q.rd.current[i] = s
		q.rd.present[i] = true
	}
	ds = rdTestViewer(q, 0)
	if err := q.sendRDEnts(ds); err != nil {
		t.Fatal(err)
	}
	prefix = rdTestMVD(rdTestServerData(0, ftePext2ReplacementDeltas, 0))
	got, chunks := rdTestDecodeSnapshot(t, append(prefix, queueBytes(ds.wb)...))
	if chunks < 2 || got.rd.time != 8 {
		t.Fatal("missing chunks/time", chunks)
	}
	for i := 1; i < 700; i++ {
		if !got.rd.present[i] || got.rd.current[i] != q.rd.current[i] {
			t.Fatal("chunk lost entity", i)
		}
	}
}

func TestRDSnapshotBoundary(t *testing.T) {
	for _, records := range [][]byte{{1, 128}, {0, 128, 255, 63, 0}} {
		q := rdWriterSource(t, false)
		ds := rdTestViewer(q, 0)
		ds.id = 1
		q.us.linkedDs = map[dStreamId]*dStream{1: ds}
		message := append([]byte{86}, rdTestFrame(records)...)
		if err := q.readMessage(message, mvdMsgRead, playersMaskAll); err != nil {
			t.Fatal(err)
		}
		packet := rdTestMVD(message)
		if err := q.us.sendMVDDataDown(packet); err != nil {
			t.Fatal(err)
		}
		if !bytes.HasSuffix(queueBytes(ds.wb), packet) {
			t.Fatal("newly active viewer did not receive the boundary record")
		}
		got, _ := rdTestDecodeSnapshot(t, queueBytes(ds.wb))
		for _, id := range []int{1, 16383} {
			if got.rd.present[id] != q.rd.present[id] || got.rd.present[id] && got.rd.current[id] != q.rd.current[id] {
				t.Fatal("startup/forward boundary changed world", id)
			}
		}
	}
}

func TestRDSnapshotPredictionBasis(t *testing.T) {
	q := rdWriterSource(t, false)
	// A later non-predicted model-angle update changes the pitch basis while
	// persistent prediction fields remain. Startup must preserve both.
	b := append([]byte{86}, rdTestFrame([]byte{1, 0, 8, 32, 0})...)
	if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	ds := rdTestViewer(q, 0)
	rdTestSignon(t, ds)
	got, _ := rdTestDecodeSnapshot(t, queueBytes(ds.wb))
	if !reflect.DeepEqual(got.rd.current[1], q.rd.current[1]) {
		t.Fatalf("changed prediction basis: got %+v want %+v", got.rd.current[1], q.rd.current[1])
	}
}

func TestRDSnapshotReferencePayload(t *testing.T) {
	for _, floats := range []bool{false, true} {
		q := rdWriterSource(t, floats)
		s := q.rd.current[1]
		msg := newNetMsgW(make([]byte, 256), floats)
		if err := q.putRDPayload(msg, s, defaultRDState()); err != nil {
			t.Fatal(err)
		}
		width := 2
		if floats {
			width = 4
		}
		want := rdTestPayload(t, width)
		if !bytes.Equal(msg.Bytes(), want) {
			t.Fatalf("reference payload differs\n got %x\nwant %x", msg.Bytes(), want)
		}
	}
}

func TestRDSnapshotFineAngles(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		q := rdWriterSource(t, false)
		// UF_PREDINFO|UF_ANGLESY, yaw short=1, no prediction subfields.
		b := append([]byte{86}, rdTestFrame([]byte{1, 0, 80, 1, 0, 0})...)
		if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil {
			t.Fatal(err)
		}
		if mixed {
			b = append([]byte{86}, rdTestFrame([]byte{1, 0, 8, 32, 0})...)
			if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil {
				t.Fatal(err)
			}
		} else {
			q.rd.current[1].pmoveType = 0
			q.rd.current[1].weaponFrame = 0
			q.rd.current[1].angles[0] = 0
			q.rd.current[1].viewAngles[0] = 0
		}
		ds := rdTestViewer(q, 0)
		rdTestSignon(t, ds)
		got, _ := rdTestDecodeSnapshot(t, queueBytes(ds.wb))
		if got.rd.current[1] != q.rd.current[1] {
			t.Fatalf("fine angle lost mixed=%v got=%+v want=%+v", mixed, got.rd.current[1], q.rd.current[1])
		}
	}
}

func TestRDSnapshotMetadataBudgets(t *testing.T) {
	q := rdWriterSource(t, false)
	name, team := strings.Repeat("x", 63), strings.Repeat("y", 53)
	for i := range maxClients {
		// Two valid values make a 128-byte userinfo. Ten player records need
		// 1460 bytes including metadata, just beyond the 1450-byte limit.
		if err := q.players[i].userInfo.Set("name", name); err != nil {
			t.Fatal(err)
		}
		if err := q.players[i].userInfo.Set("team", team); err != nil {
			t.Fatal(err)
		}
	}
	for i := range maxLightStyles {
		q.lightStyle[i] = strings.Repeat("m", 30)
	}
	ds := rdTestViewer(q, 0)
	rdTestSignon(t, ds)
	data := queueBytes(ds.wb)
	decoded, _ := rdTestDecodeSnapshot(t, data)
	for i := range maxClients {
		if decoded.players[i].userInfo.Get("name") != name || decoded.players[i].userInfo.Get("team") != team {
			t.Fatalf("metadata chunks lost player %d", i)
		}
	}
	for i := range maxLightStyles {
		if decoded.lightStyle[i] != strings.Repeat("m", 30) {
			t.Fatalf("metadata chunks lost lightstyle %d", i)
		}
	}
	for len(data) > 0 {
		if len(data) < 6 {
			t.Fatal("short metadata header")
		}
		n := int(binary.LittleEndian.Uint32(data[2:6]))
		if n < 0 || n > maxMsgSize || n > len(data)-6 {
			t.Fatalf("invalid metadata chunk size: %d", n)
		}
		data = data[6+n:]
	}
}

func TestRDSnapshotClassicBaseline(t *testing.T) {
	q := rdWriterSource(t, false)
	b := []byte{22, 1, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0}
	if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	b = append([]byte{86}, rdTestFrame([]byte{0, 128, 1, 0, 128, 1})...)
	if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	ds := rdTestViewer(q, 0)
	rdTestSignon(t, ds)
	got, _ := rdTestDecodeSnapshot(t, queueBytes(ds.wb))
	if got.rd.current[1] != q.rd.current[1] {
		t.Fatalf("classic inheritance changed got=%+v want=%+v", got.rd.current[1], q.rd.current[1])
	}
}

// Explicit zero must replace a previously retained vwep, including when startup
// inherits a nonzero vwep/alpha from a baseline instead of the null state.
func TestRDSnapshotVwepTransitions(t *testing.T) {
	for _, ext := range []uint32{0, qtvEzQuakeExtDownload} {
		t.Run(fmt.Sprintf("download=%v", ext != 0), func(t *testing.T) {
			q := rdWriterSource(t, false)
			baseline := append([]byte{66, 1, 0}, rdTestPayload(t, 2)...)
			if err := q.readMessage(baseline, mvdMsgRead, playersMaskAll); err != nil {
				t.Fatal(err)
			}
			for _, state := range []struct {
				model uint16
				alpha byte
			}{{511, 64}, {0, 255}} {
				// UF_WIDE|UF_ALPHA|UF_MODELINDEX2 with all three extension bytes.
				record := []byte{1, 0, 0x80, 0x82, 0x81, 0x08, state.alpha, byte(state.model), byte(state.model >> 8)}
				update := append([]byte{86}, rdTestFrame(record)...)
				if err := q.readMessage(update, mvdMsgRead, playersMaskAll); err != nil {
					t.Fatal(err)
				}
				// A following update omits these persistent fields.
				omitted := append([]byte{86}, rdTestFrame([]byte{1, 0, 0})...)
				if err := q.readMessage(omitted, mvdMsgRead, playersMaskAll); err != nil {
					t.Fatal(err)
				}
				if got := q.rd.current[1]; got.modelIndex2 != state.model || got.trans != state.alpha {
					t.Fatalf("source lost vwep/alpha: got %+v want model=%d alpha=%d", got, state.model, state.alpha)
				}
				ds := rdTestViewer(q, ext)
				rdTestSignon(t, ds)
				decoded, _ := rdTestDecodeSnapshot(t, queueBytes(ds.wb))
				if got := decoded.rd.current[1]; !decoded.rd.present[1] || got.modelIndex2 != state.model || got.trans != state.alpha {
					t.Fatalf("late viewer lost vwep/alpha: got %+v want model=%d alpha=%d", got, state.model, state.alpha)
				}
				if got := decoded.rd.baselines[1]; !decoded.rd.baselinePresent[1] || got.modelIndex2 != 400 || got.trans != 127 {
					t.Fatalf("startup changed the baseline: %+v", got)
				}
			}
		})
	}
}
