package qtv

import (
	"bytes"
	"fmt"
	"testing"
)

func TestRDPendingSignon(t *testing.T) {
	for _, ext := range []uint32{0, qtvEzQuakeExtDownload} {
		for _, baseline := range []bool{false, true} {
			t.Run(fmt.Sprintf("download=%v/baseline=%v", ext != 0, baseline), func(t *testing.T) {
				q := rdTestProtocol()
				q.us.mvdHdr = newNetMsgW(make([]byte, 16), false)
				if baseline {
					// A baseline allocates retained storage before the first world reset.
					if err := q.readMessage([]byte{66, 1, 0, 0}, mvdMsgRead, playersMaskAll); err != nil {
						t.Fatal(err)
					}
					if q.rd == nil || q.rd.ready || !q.rd.baselinePresent[1] {
						t.Fatal("baseline did not create an unready RD world")
					}
				}
				ds := rdTestViewer(q, ext)
				if err := q.us.sendInitialMVDData(ds); err != nil || ds.state != dsNeedInitialData || ds.wb.Length() != 0 {
					t.Fatalf("unready startup: state=%v err=%v bytes=%d", ds.state, err, ds.wb.Length())
				}
				ds.state = dsNeedSpawn
				command := fmt.Sprintf("qtvspawn %d", q.us.serverCount)
				if err := ds.executeClientCommand(command); err != nil || ds.state != dsNeedInitialData || ds.wb.Length() != 0 {
					t.Fatalf("unready spawn: state=%v err=%v bytes=%d", ds.state, err, ds.wb.Length())
				}
				reset := append([]byte{86}, rdTestFrame([]byte{0, 128})...)
				if err := q.readMessage(reset, mvdMsgRead, playersMaskAll); err != nil {
					t.Fatal(err)
				}
				rdTestSignon(t, ds)
				if ds.wb.Length() == 0 {
					t.Fatal("ready world did not produce startup data")
				}
			})
		}
	}
}

func TestRDMapEpoch(t *testing.T) {
	q := rdTestProtocol()
	q.us.wb = mustByteQueue(4096, 4*1024)
	q.us.mvdHdr = newNetMsgW(make([]byte, 16), false)
	plain := rdTestViewer(q, 0)
	plain.state = dsActive
	download := rdTestViewer(q, qtvEzQuakeExtDownload)
	download.state = dsNeedModelList
	q.us.linkedDs = map[dStreamId]*dStream{1: plain, 2: download}
	for index, mask := range []uint32{0, ftePext2ReplacementDeltas, ftePext2ReplacementDeltas, 0} {
		download.state = []dStreamState{dsNeedSoundList, dsNeedModelList, dsNeedSpawn, dsActive}[index]
		q.soundList[1] = "old"
		q.modelList[1] = "old"
		q.extFlagsMVD1 = mvdPext1FloatCoords
		if err := q.ensureRD().apply(rdFrame{reset: true, time: 12}); err != nil {
			t.Fatal(err)
		}
		oldCount := q.us.serverCount
		if err := q.readMessage(rdTestServerData(0, mask, 0), mvdMsgRead, playersMaskAll); err != nil {
			t.Fatal(err)
		}
		if q.rd != nil || q.extFlagsMVD1 != 0 || q.soundList[1] != "" || q.modelList[1] != "" || q.us.serverCount != oldCount+1 || q.usesRD() != (mask != 0) {
			t.Fatal("stale map storage")
		}
		if download.state != dsNeedInitialData || plain.state != dsActive {
			t.Fatal("map viewer states")
		}
		// Test old map commands during the matching stage of the new signon,
		// so a wrong-state fallback cannot hide a missing generation check.
		q.us.state = usActive
		if q.usesRD() {
			if err := q.ensureRD().apply(rdFrame{reset: true}); err != nil {
				t.Fatal(err)
			}
		}
		for _, command := range []struct {
			name        string
			stage, next dStreamState
		}{
			{"qtvsoundlist", dsNeedSoundList, dsNeedModelList},
			{"qtvmodellist", dsNeedModelList, dsNeedSpawn},
			{"qtvspawn", dsNeedSpawn, dsActive},
		} {
			download.state = command.stage
			before := bytes.Clone(queueBytes(download.wb))
			stale := fmt.Sprintf("%s %d", command.name, oldCount)
			if err := download.executeClientCommand(stale); err != nil || download.state != command.stage || !bytes.Equal(before, queueBytes(download.wb)) {
				t.Fatal("stale command changed the new signon", stale, err)
			}
			current := fmt.Sprintf("%s %d", command.name, q.us.serverCount)
			if err := download.executeClientCommand(current); err != nil || download.state != command.next || download.wb.Length() <= len(before) {
				t.Fatal("current map command did not advance signon", current, err)
			}
		}
	}
	q.us.rb = mustByteQueue(1024, 64*1024)
	if err := q.ensureRD().apply(rdFrame{reset: true}); err != nil {
		t.Fatal(err)
	}
	if err := q.us.setState(usInitial); err != nil || q.rd != nil || plain.state != dsNeedInitialData {
		t.Fatal("source reconnect retained map", err)
	}
}
