package qtv

import (
	"bytes"
	"testing"
	"time"
)

func TestRDSignonPagesDenseWorld(t *testing.T) {
	us := denseQueueTestStream()
	us.qp.isPaused = true
	if err := us.qp.serverInfo.FromStr(`\status\countdown\matchtag\final`); err != nil {
		t.Fatal(err)
	}
	const budget = 192 * 1024
	first := rdTestViewer(us.qp, 0)
	second := rdTestViewer(us.qp, qtvEzQuakeExtDownload)
	first.wb = mustByteQueue(budget, 16*1024)
	second.wb = mustByteQueue(budget, 16*1024)
	first.id, second.id = 1, 2
	us.linkedDs = map[dStreamId]*dStream{1: first, 2: second}
	for _, ds := range []*dStream{first, second} {
		if err := us.sendInitialMVDData(ds); err != nil {
			t.Fatalf("start dense signon: %v", err)
		}
	}
	for _, name := range []string{"qtvsoundlist 7", "qtvmodellist 7", "qtvspawn 7"} {
		if err := second.executeClientCommand(name); err != nil {
			t.Fatal(name, err)
		}
	}
	if first.state == dsActive || second.state == dsActive {
		t.Fatal("dense signon should need more than one queue fill")
	}

	msg := us.qp.w.Clear()
	msg.PutSVC(svc_serverinfo)
	msg.PutString("status")
	msg.PutString("standby")
	msg.PutSVC(svc_serverinfo)
	msg.PutString("matchtag")
	msg.PutString("")
	if err := us.qp.readMessage(msg.Bytes(), mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	us.qp.isPaused = false
	for _, ds := range []*dStream{first, second} {
		wire := drainRDSignon(t, ds)
		got, _ := rdTestDecodeSnapshot(t, wire)
		unpause := rdTestMVD([]byte{byte(svc_setpause), 0})
		unpause[1] = byte(mvdMsgRead)
		if got.isPaused || bytes.Contains(wire, unpause) {
			t.Error("viewer should start unpaused without an explicit unpause")
		}
		if status := got.serverInfo.Get("status"); status != "standby" {
			t.Errorf("viewer %d received status %q, want standby", ds.id, status)
		}
		if _, ok := got.serverInfo.GetOK("matchtag"); ok {
			t.Errorf("viewer %d retained removed serverinfo key matchtag", ds.id)
		}
	}
}

// drainRDSignon models a viewer draining each queued batch before the next tick.
func drainRDSignon(t *testing.T, ds *dStream) []byte {
	t.Helper()
	us := ds.linkedUs
	if us.linkedDs == nil {
		us.linkedDs = map[dStreamId]*dStream{ds.id: ds}
	}
	var wire []byte
	for range 128 {
		wire = append(wire, queueBytes(ds.wb)...)
		if err := ds.wb.Discard(ds.wb.Length()); err != nil {
			t.Fatal(err)
		}
		if ds.state == dsActive {
			return wire
		}
		if ds.isCanceled() {
			t.Fatal("signon viewer canceled")
		}
		us.pumpRDSignons()
	}
	t.Fatal("signon did not finish")
	return nil
}

func TestRDSignonCorrectsSharedWorld(t *testing.T) {
	us := denseQueueTestStream()
	qp := us.qp
	qp.players[0].userInfo.Set("name", "player")
	qp.players[0].stats[0] = 100
	high := uint32(maxRDEntities - 1)
	first := rdTestViewer(qp, 0)
	second := rdTestViewer(qp, 0)
	first.id, second.id = 1, 2
	first.wb = mustByteQueue(192*1024, 16*1024)
	second.wb = mustByteQueue(256*1024, 16*1024)
	us.linkedDs = map[dStreamId]*dStream{first.id: first, second.id: second}
	us.pumpRDSignons()
	if !first.rdSignon.attached || !second.rdSignon.attached ||
		first.rdSignon.offset == second.rdSignon.offset {
		t.Fatal("viewers should share one snapshot at different offsets")
	}

	// Clear the initial world, then reuse a high ID while signon is in progress.
	msg := qp.w.Clear()
	msg.PutSVC(svc_fte_updateentities)
	msg.PutFloat32(123)
	msg.PutUint16(rdUpdateRemove)
	msg.PutRDUpdateIndex(high, false)
	state := qp.rd.current[high]
	state.origin[0] = 177
	if err := qp.putRDPayload(msg, state, defaultRDState()); err != nil {
		t.Fatal(err)
	}
	msg.PutUint16(0)
	if err := qp.readMessage(msg.Bytes(), mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	qp.players[0].stats[0] = 0
	qp.lightStyle[0] = "abc"
	qp.isPaused = true
	for _, ds := range []*dStream{first, second} {
		// The full reset cleared too many IDs for a tiny queue, but 192 KiB
		// is sufficient for their removal indices plus current metadata.
		data := drainRDSignon(t, ds)
		got, _ := rdTestDecodeSnapshot(t, data)
		if got.rd.present[2] || !got.rd.present[high] || got.rd.current[high].origin[0] != 177 ||
			got.players[0].stats[0] != 0 || got.lightStyle[0] != "abc" || !got.isPaused {
			t.Fatal("correction did not restore the current world and HUD")
		}
		if bytes.Count(data, []byte("skins\n\x00")) != 1 {
			t.Fatal("expected one signon completion")
		}
	}
	if us.rdSignon.data != nil {
		t.Fatal("snapshot retained after last viewer finished")
	}
}

func TestRDSignonDefaultJoinWindow(t *testing.T) {
	us := denseQueueTestStream()
	newViewer := func() *dStream {
		ds := rdTestViewer(us.qp, 0)
		ds.wb = mustByteQueue(192*1024, 16*1024)
		return ds
	}
	start := time.Unix(0, 0)
	if err := us.sendRDSignon(newViewer(), start); err != nil {
		t.Fatal(err)
	}
	before, at := newViewer(), newViewer()
	cutoff := start.Add(3 * time.Second)
	if err := us.sendRDSignon(before, cutoff.Add(-time.Nanosecond)); err != nil || !before.rdSignon.attached {
		t.Fatalf("viewer just before the cutoff should join the snapshot: %v", err)
	}
	if err := us.sendRDSignon(at, cutoff); err != nil || at.rdSignon.attached || at.wb.Length() != 0 {
		t.Fatalf("viewer at the cutoff should wait for the next snapshot: %v", err)
	}
	deadline := start.Add(12 * time.Second)
	if err := us.sendRDSignon(before, deadline.Add(-time.Nanosecond)); err != nil {
		t.Fatalf("viewer should keep its place until the shared deadline: %v", err)
	}
	if err := us.sendRDSignon(before, deadline); err == nil {
		t.Fatal("late join extended the shared deadline")
	}
}

func TestRDSignonDeadlineAndNextGroup(t *testing.T) {
	us := denseQueueTestStream()
	us.qtv.qvs.Set(rdSignonJoinVar, "0")
	slow := rdTestViewer(us.qp, 0)
	next := rdTestViewer(us.qp, 0)
	slow.wb = mustByteQueue(192*1024, 16*1024)
	next.wb = mustByteQueue(192*1024, 16*1024)
	slow.id, next.id = 1, 2
	us.linkedDs = map[dStreamId]*dStream{slow.id: slow}
	us.pumpRDSignons()
	if !slow.rdSignon.attached {
		t.Fatal("first viewer did not start")
	}
	us.linkedDs[next.id] = next
	us.pumpRDSignons()
	if next.rdSignon.attached || next.isCanceled() {
		t.Fatal("viewer should wait for the next group")
	}
	us.rdSignon.deadline = time.Now().Add(-time.Second)
	us.pumpRDSignons()
	if !slow.isCanceled() || next.isCanceled() {
		t.Fatal("deadline must isolate the slow viewer")
	}
	drainRDSignon(t, next)
}

func TestRDSignonMapChangeDiscardsPendingSnapshot(t *testing.T) {
	us := denseQueueTestStream()
	us.wb = mustByteQueue(4096, 512)
	viewer := rdTestViewer(us.qp, 0)
	viewer.wb = mustByteQueue(192*1024, 16*1024)
	us.linkedDs = map[dStreamId]*dStream{viewer.id: viewer}
	us.pumpRDSignons()
	if !viewer.rdSignon.attached {
		t.Fatal("old map signon did not start")
	}
	tail := bytes.Clone(queueBytes(viewer.wb))
	legacy := rdTestServerData(0, 0, 0)
	if err := us.qp.readMessage(legacy, mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	if us.rdSignon.data != nil || viewer.rdSignon.attached || viewer.state != dsNeedInitialData ||
		!bytes.Equal(queueBytes(viewer.wb), tail) {
		t.Fatal("map transition retained the old snapshot or discarded queued bytes")
	}
	if err := viewer.wb.Discard(len(tail)); err != nil {
		t.Fatal(err)
	}
	skins := []byte{byte(svc_stufftext), 's', 'k', 'i', 'n', 's', '\n', 0}
	if err := us.qp.readMessage(skins, mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	live := rdTestMVD([]byte{byte(svc_cdtrack), 7})
	if err := us.sendMVDDataDown(live); err != nil {
		t.Fatal(err)
	}
	got, _ := rdTestDecodeSnapshot(t, queueBytes(viewer.wb))
	if viewer.isCanceled() || viewer.state != dsActive || got.usesRD() || got.cdTrack != 7 {
		t.Fatal("viewer did not sign on and resume in legacy")
	}
}

func TestRDSignonOversizedCorrectionIsolatesViewer(t *testing.T) {
	us := denseQueueTestStream()
	slow := rdTestViewer(us.qp, 0)
	slow.wb = mustByteQueue(32*1024, 4096)
	healthy := rdTestViewer(us.qp, 0)
	healthy.id = 2
	healthy.state = dsActive
	us.linkedDs = map[dStreamId]*dStream{slow.id: slow, healthy.id: healthy}
	us.pumpRDSignons()
	if !slow.rdSignon.attached {
		t.Fatal("signon did not start")
	}
	for id := range us.qp.rd.present {
		if id != 0 {
			us.rdSignon.dirty.set(uint32(id))
		}
	}
	for range 128 {
		if err := slow.wb.Discard(slow.wb.Length()); err != nil {
			t.Fatal(err)
		}
		us.pumpRDSignons()
		if slow.isCanceled() {
			break
		}
	}
	record := rdTestMVD([]byte{byte(svc_cdtrack), 7})
	if err := us.sendMVDDataDown(record); err != nil {
		t.Fatal(err)
	}
	if !slow.isCanceled() || slow.state == dsActive || healthy.isCanceled() ||
		!bytes.Equal(queueBytes(healthy.wb), record) || us.rdSignon.data != nil {
		t.Fatal("oversized correction should disconnect only its viewer")
	}
}

func TestRDSignonTimeSettings(t *testing.T) {
	us := denseQueueTestStream()
	vars := us.qtv.qvs
	vars.Set(rdSignonTimeoutVar, "0.25")
	vars.Set(rdSignonJoinVar, "NaN")
	if vars.Get(rdSignonTimeoutVar).Float != 12 || vars.Get(rdSignonJoinVar).Float != 3 {
		t.Fatal("invalid duration pair or NaN was accepted")
	}
	vars.Set(rdSignonJoinVar, "0.125")
	vars.Set(rdSignonTimeoutVar, "0.375")
	viewer := rdTestViewer(us.qp, 0)
	viewer.wb = mustByteQueue(192*1024, 16*1024)
	us.linkedDs = map[dStreamId]*dStream{viewer.id: viewer}
	us.pumpRDSignons()
	original := us.rdSignon.deadline
	if original.Sub(us.rdSignon.joinUntil) != 250*time.Millisecond {
		t.Fatal("fractional durations were not applied")
	}
	vars.Set(rdSignonTimeoutVar, "2")
	us.pumpRDSignons()
	if us.rdSignon.deadline != original {
		t.Fatal("settings updates should only affect new snapshots")
	}
}

func TestRDSignonHUDUsesCurrentPlayers(t *testing.T) {
	for _, tc := range []struct {
		name     string
		spectate bool
		reuse    bool
	}{
		{name: "disconnect"},
		{name: "spectate", spectate: true},
		{name: "reuse after disconnect", reuse: true},
		{name: "reuse after spectating", spectate: true, reuse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			us := denseQueueTestStream()
			qp := us.qp
			const slot = 31
			const health, armor = 0, 4
			updateUser := func(id uint32, info string) {
				t.Helper()
				msg := qp.w.Clear()
				msg.PutSVC(svc_updateuserinfo)
				msg.PutByte(slot)
				msg.PutUint32(id)
				msg.PutString(info)
				if err := qp.readMessage(msg.Bytes(), mvdMsgAll, playersMaskAll); err != nil {
					t.Fatal(err)
				}
			}
			updateStats := func(payload ...byte) {
				t.Helper()
				if err := qp.readMessage(payload, mvdMsgStats, 1<<slot); err != nil {
					t.Fatal(err)
				}
			}
			updateUser(1, `\name\original`)
			updateStats(byte(svc_updatestat), health, 100, byte(svc_updatestat), armor, 75)
			ds := rdTestViewer(qp, 0)
			ds.wb = mustByteQueue(192*1024, 16*1024)
			if err := us.startRDSignon(ds); err != nil || !ds.rdSignon.attached {
				t.Fatalf("expected paged signon: %v", err)
			}
			initial, _ := rdTestDecodeSnapshot(t, us.rdSignon.data)
			if got := initial.players[slot].stats[health]; got != 0 {
				t.Errorf("snapshot sent old health %d before the final userinfo was known", got)
			}

			var wantName string
			var wantID, wantHealth, wantArmor uint32
			if tc.spectate {
				wantID, wantName = 1, "original"
				updateUser(1, `\name\original\*spectator\1`)
			} else {
				updateUser(0, "")
			}
			if tc.reuse {
				wantID, wantName, wantHealth, wantArmor = 2, "replacement", 125, 75
				updateUser(2, `\name\replacement`)
				// MVD's delta is relative to the previous values in this slot.
				updateStats(byte(svc_updatestat), health, 125)
			}

			got, _ := rdTestDecodeSnapshot(t, drainRDSignon(t, ds))
			player := &got.players[slot]
			if player.userId != wantID || player.userInfo.Get("name") != wantName || player.isPlayer() != tc.reuse {
				t.Errorf("unexpected userinfo for slot %d: id=%d %q", slot, player.userId, player.userInfo.String())
			}
			if player.stats[health] != wantHealth || player.stats[armor] != wantArmor {
				t.Errorf("health/armor = %d/%d, want %d/%d", player.stats[health], player.stats[armor], wantHealth, wantArmor)
			}
		})
	}
}

func TestRDSignonSendsZeroViewHeight(t *testing.T) {
	us := denseQueueTestStream()
	us.qp.players[0].userInfo.Set("name", "player")
	ds := rdTestViewer(us.qp, 0)
	ds.wb = mustByteQueue(192*1024, 16*1024)
	if err := us.startRDSignon(ds); err != nil || !ds.rdSignon.attached {
		t.Fatalf("expected paged signon: %v", err)
	}
	// ezQuake initializes STAT_VIEWHEIGHT to 22 on serverdata. It needs an
	// explicit update even when the current RD view height is zero.
	const viewHeight = 16
	data := drainRDSignon(t, ds)
	for len(data) != 0 {
		cursor := byteViewOf(data).cursor(0)
		header, status := inspectMVDRecord(&cursor, false)
		if status != mvdRecordComplete {
			t.Fatal("invalid generated MVD")
		}
		payload := header.quakePayload(data, false)
		if header.kind == mvdMsgStats && header.playersMask == 1 &&
			bytes.Contains(payload, []byte{byte(svc_updatestat), viewHeight, 0}) {
			return
		}
		data = data[header.size:]
	}
	t.Fatal("missing explicit zero view height for player 0")
}
