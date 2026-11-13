package qtv

import (
	"fmt"
	"testing"
)

func TestGeneratedSignonTrack(t *testing.T) {
	for _, rd := range []bool{false, true} {
		for _, ext := range []uint32{0, qtvEzQuakeExtDownload} {
			t.Run(fmt.Sprintf("RD=%v/download=%v", rd, ext != 0), func(t *testing.T) {
				q := rdWriterSource(t, false)
				if !rd {
					q.extFlagsFTE2 = 0
				}
				if err := q.readMessage([]byte{byte(svc_cdtrack), 2}, mvdMsgRead, playersMaskAll); err != nil {
					t.Fatal(err)
				}
				viewer := rdTestViewer(q, ext)
				rdTestSignon(t, viewer)
				got, _ := rdTestDecodeSnapshot(t, queueBytes(viewer.wb))
				if !got.hasCDTrack || got.cdTrack != 2 {
					t.Fatalf("track = %d (present=%t), want 2", got.cdTrack, got.hasCDTrack)
				}
			})
		}
	}
}

func TestGeneratedSignonTrackChangesBeforeSpawn(t *testing.T) {
	for _, track := range []byte{4, 0} {
		t.Run(fmt.Sprintf("track=%d", track), func(t *testing.T) {
			q := rdWriterSource(t, false)
			us := q.us
			viewer := rdTestViewer(q, qtvEzQuakeExtDownload)
			us.mu.Lock()
			defer us.mu.Unlock()
			if err := q.readMessage([]byte{byte(svc_cdtrack), 2}, mvdMsgRead, playersMaskAll); err != nil {
				t.Fatal(err)
			}
			if err := us.sendInitialMVDData(viewer); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"qtvsoundlist", "qtvmodellist"} {
				if err := viewer.executeClientCommand(fmt.Sprintf("%s %d", name, us.serverCount)); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := rdTestDecodeSnapshot(t, queueBytes(viewer.wb))
			if err := q.readMessage([]byte{byte(svc_cdtrack), track}, mvdMsgRead, playersMaskAll); err != nil {
				t.Fatal(err)
			}
			if err := viewer.executeClientCommand(fmt.Sprintf("qtvspawn %d", us.serverCount)); err != nil {
				t.Fatal(err)
			}
			if viewer.state != dsActive {
				t.Fatalf("viewer state = %v, want active", viewer.state)
			}
			after, _ := rdTestDecodeSnapshot(t, queueBytes(viewer.wb))
			if !after.hasCDTrack || after.cdTrack != track {
				t.Fatalf("track after spawn = %d (present=%t), want %d", after.cdTrack, after.hasCDTrack, track)
			}
			if before.hasCDTrack {
				t.Fatalf("track %d sent before spawn", before.cdTrack)
			}
		})
	}
}

func TestGeneratedSignonTrackMapChange(t *testing.T) {
	for _, tc := range []struct {
		name     string
		hasTrack bool
	}{
		{name: "no track"},
		{name: "explicit zero", hasTrack: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := rdWriterSource(t, false)
			q.extFlagsFTE2 = 0
			us := q.us
			us.rb = mustByteQueue(4096, 64)
			us.wb = mustByteQueue(4096, 64)
			if err := q.readMessage([]byte{byte(svc_cdtrack), 2}, mvdMsgAll, playersMaskAll); err != nil {
				t.Fatal(err)
			}
			viewer := rdTestViewer(q, qtvEzQuakeExtDownload)
			rdTestSignon(t, viewer)
			us.linkedDs = map[dStreamId]*dStream{viewer.id: viewer}
			if err := viewer.wb.Discard(viewer.wb.Length()); err != nil {
				t.Fatal(err)
			}

			startup := rdTestMVD(rdTestServerData(0, 0, 0))
			if tc.hasTrack {
				startup = append(startup, rdTestMVD([]byte{byte(svc_cdtrack), 0})...)
			}
			startup = append(startup, rdTestMVD(append([]byte{byte(svc_stufftext)}, "skins\n\x00"...))...)
			if _, err := us.rb.WriteFull(startup); err != nil {
				t.Fatal(err)
			}
			if err := us.parseMVD(); err != nil {
				t.Fatal(err)
			}
			if viewer.state != dsNeedSoundList {
				t.Fatalf("viewer state = %v", viewer.state)
			}
			us.mu.Lock()
			defer us.mu.Unlock()
			for _, name := range []string{"qtvsoundlist", "qtvmodellist", "qtvspawn"} {
				if err := viewer.executeClientCommand(fmt.Sprintf("%s %d", name, us.serverCount)); err != nil {
					t.Fatal(err)
				}
			}
			got, _ := rdTestDecodeSnapshot(t, queueBytes(viewer.wb))
			if got.hasCDTrack != tc.hasTrack || got.cdTrack != 0 {
				t.Fatalf("track = %d (present=%t), want 0 (present=%t)", got.cdTrack, got.hasCDTrack, tc.hasTrack)
			}
		})
	}
}
