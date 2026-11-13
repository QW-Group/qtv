package qtv

import (
	"bytes"
	"fmt"
	"testing"
)

func TestGeneratedMapSignon(t *testing.T) {
	for _, rd := range []bool{false, true} {
		t.Run(fmt.Sprintf("RD=%t", rd), func(t *testing.T) {
			q := rdWriterSource(t, false)
			us := q.us
			us.rb = mustByteQueue(4096, 64)
			us.wb = mustByteQueue(4096, 64)
			viewer := rdTestViewer(q, 0)
			(&dStreamStorage{}).regVars(us.qtv)
			viewer.wb = mustByteQueue(us.qtv.qvs.Get("dstream_write_buf_size").Int, 16*1024)
			rdTestSignon(t, viewer)
			us.linkedDs = map[dStreamId]*dStream{viewer.id: viewer}
			if err := viewer.wb.Discard(viewer.wb.Length()); err != nil {
				t.Fatal(err)
			}

			feed := func(record []byte) {
				t.Helper()
				if _, err := us.rb.WriteFull(record); err != nil {
					t.Fatal(err)
				}
				if err := us.parseMVD(); err != nil {
					t.Fatal(err)
				}
			}
			var bits uint32
			if rd {
				bits = ftePext2ReplacementDeltas
			}
			feed(rdTestMVD(rdTestServerData(0, bits, 0)))
			if viewer.state != dsNeedInitialData || viewer.wb.Length() != 0 {
				t.Fatalf("new serverdata reached viewer before signon was ready, state=%v", viewer.state)
			}
			feed(rdTestMVD([]byte{byte(svc_cdtrack), 4}))
			feed(rdTestMVD(append([]byte{byte(svc_stufftext)}, "skins\n\x00"...)))
			if rd {
				if viewer.state != dsNeedInitialData || viewer.wb.Length() != 0 {
					t.Fatal("RD signon was sent before the first world reset")
				}
				feed(rdTestMVD(append([]byte{byte(svc_fte_updateentities)}, rdTestFrame([]byte{0, 128})...)))
			}
			if viewer.state != dsActive || viewer.isCanceled() {
				t.Fatalf("viewer did not resume on the same connection, state=%v", viewer.state)
			}
			got, _ := rdTestDecodeSnapshot(t, queueBytes(viewer.wb))
			if got.mapName != "map" || got.usesRD() != rd || !got.hasCDTrack || got.cdTrack != 4 {
				t.Fatalf("wrong generated signon: map=%q RD=%t track=%d (present=%t)", got.mapName, got.usesRD(), got.cdTrack, got.hasCDTrack)
			}
			live := rdTestMVD([]byte{byte(svc_nop)})
			feed(live)
			if !bytes.HasSuffix(queueBytes(viewer.wb), live) {
				t.Fatal("live forwarding did not resume after signon")
			}
		})
	}
}
