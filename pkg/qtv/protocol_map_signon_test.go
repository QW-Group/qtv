package qtv

import (
	"bytes"
	"testing"
)

func TestMapResetClearsRemoteHostNameConcurrently(t *testing.T) {
	us, _ := bufferingTestStream(t, 0, false)
	us.linkedDs = nil
	us.qp.remoteHostName.Store("previous source name")

	// SOURCELIST reads the hostname without taking the upstream mutex.
	readDone := make(chan struct{})
	go func() {
		us.qp.remoteHostName.Load()
		close(readDone)
	}()
	err := us.setState(usParsingConnection)
	<-readDone
	if err != nil {
		t.Fatal(err)
	}
	if got := us.qp.remoteHostName.Load(); got != "" {
		t.Fatalf("map reset retained upstream hostname %q", got)
	}
}

func TestGeneratedMapSignon(t *testing.T) {
	for _, tc := range []struct {
		name       string
		extensions uint32
		resetFirst bool
	}{
		{"legacy", 0, false},
		{"RD/skins-first", ftePext2ReplacementDeltas, false},
		{"RD/reset-first", ftePext2ReplacementDeltas, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			rd := tc.extensions != 0
			feed(rdTestMVD(rdTestServerData(0, tc.extensions, 0)))
			if viewer.state != dsNeedInitialData || viewer.wb.Length() != 0 {
				t.Fatalf("new serverdata reached viewer before signon was ready, state=%v", viewer.state)
			}
			feed(rdTestMVD([]byte{byte(svc_cdtrack), 4}))
			ready := rdTestMVD(append([]byte{byte(svc_stufftext)}, "skins\n\x00"...))
			if rd {
				reset := rdTestMVD(append([]byte{byte(svc_fte_updateentities)}, rdTestFrame([]byte{0, 128})...))
				if tc.resetFirst {
					feed(reset)
				} else {
					feed(ready)
					ready = reset
				}
				if viewer.state != dsNeedInitialData || viewer.wb.Length() != 0 {
					t.Fatal("RD signon was sent before both skins and the first world reset")
				}
			}
			live := rdTestMVD(append([]byte{byte(svc_print), byte(printHigh)}, "Match started\n\x00"...))
			feed(append(ready, live...))
			if viewer.state != dsActive || viewer.isCanceled() {
				t.Fatalf("viewer did not resume on the same connection, state=%v", viewer.state)
			}
			wire := queueBytes(viewer.wb)
			got, chunks := rdTestDecodeSnapshot(t, wire)
			if got.mapName != "map" || got.usesRD() != rd || !got.hasCDTrack || got.cdTrack != 4 {
				t.Fatalf("wrong generated signon: map=%q RD=%t track=%d (present=%t)", got.mapName, got.usesRD(), got.cdTrack, got.hasCDTrack)
			}
			if !bytes.HasSuffix(wire, live) || bytes.Count(wire, live) != 1 {
				t.Fatal("first live record in the same parse pass must follow signon exactly once")
			}
			if rd && (chunks != 1 || bytes.Count(wire, []byte("skins\n\x00")) != 1) {
				t.Fatal("RD viewer received the signon boundary record twice")
			}
		})
	}
}
