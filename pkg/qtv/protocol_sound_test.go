package qtv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/qqshka/ringbuffer"
)

// soundTestProtocol creates the real reader/writer without an upstream network connection.
func soundTestProtocol() *qProtocol {
	qp := &qProtocol{r: newNetMsgR(nil, false), w: newNetMsgW(make([]byte, maxMvdSize), false)}
	qp.extFlagsFTE1 = 0x80
	return qp
}

// TestSoundExtendedParsing checks independent literal wire packets and the following svc.
func TestSoundExtendedParsing(t *testing.T) {
	for _, floats := range []bool{false, true} {
		for _, flags := range []byte{0, 1, 2, 3, 8, 16, 24, 27, 255} {
			t.Run(fmt.Sprintf("floats=%v/flags=%d", floats, flags), func(t *testing.T) {
				wire := []byte{55, flags}
				if flags&4 != 0 {
					wire = append(wire, 1) // FTE extra flags: velocity follows.
				}
				if flags&1 != 0 {
					wire = append(wire, 127)
				}
				if flags&2 != 0 {
					wire = append(wire, 32)
				}
				if flags&128 != 0 {
					wire = append(wire, 120) // Pitch adjustment.
				}
				if flags&64 != 0 {
					wire = append(wire, 1, 0) // Signed time offset.
				}
				if flags&4 != 0 {
					wire = append(wire, 1, 0, 2, 0, 3, 0) // Velocity.
				}
				if flags&32 != 0 {
					wire = append(wire, 0xa0, 0x0f) // Speed * 4000.
				}
				wire = append(wire, 8, 0) // Packed entity/channel or separate entity.
				if flags&8 != 0 {
					wire = append(wire, 7) // Separate channel.
				}
				if flags&16 != 0 {
					wire = append(wire, 255, 15) // Sound 4095, little endian.
				} else {
					wire = append(wire, 255)
				}
				for _, coord := range []float32{1, -2, 3} {
					if floats {
						wire = binary.LittleEndian.AppendUint32(wire, math.Float32bits(coord))
					} else {
						wire = binary.LittleEndian.AppendUint16(wire, uint16(int16(coord*8)))
					}
				}
				packet := append(append([]byte(nil), wire...), 32, 7) // svc_cdtrack, marker.
				qp := soundTestProtocol()
				if floats {
					qp.extFlagsFTE1 |= 0x8000
				}
				if err := qp.readMessage(packet, mvdMsgRead, playersMaskAll); err != nil || qp.cdTrack != 7 {
					t.Fatalf("packet parse: %v, next svc marker=%d", err, qp.cdTrack)
				}
				for end := 1; end < len(wire); end++ {
					if err := qp.readMessage(wire[:end], mvdMsgRead, playersMaskAll); err == nil {
						t.Fatalf("accepted truncated sound at byte %d", end)
					}
				}
			})
		}
	}
}

// TestSoundExtendedRequiresExtension prevents parsing an unadvertised wire layout.
func TestSoundExtendedRequiresExtension(t *testing.T) {
	qp := soundTestProtocol()
	qp.extFlagsFTE1 = 0
	if err := qp.readMessage([]byte{55, 0, 8, 0, 1, 0, 0, 0, 0, 0, 0}, mvdMsgRead, playersMaskAll); err == nil {
		t.Fatal("accepted soundextended without SOUNDDBL")
	}
}

// TestExtendedSoundCatalog accepts wire cursor 256 and the highest MVDSV sound slot.
func TestExtendedSoundCatalog(t *testing.T) {
	for _, cursor := range []int{255, 256, 4094} {
		qp := soundTestProtocol()
		wire := []byte{56, byte(cursor), byte(cursor >> 8), 'a', 0, 0, 0}
		if cursor == 255 {
			wire = []byte{46, 255, 'a', 0, 0, 0}
		}
		if err := qp.readMessage(wire, mvdMsgRead, playersMaskAll); err != nil {
			t.Fatalf("cursor %d: %v", cursor, err)
		}
		if cursor+1 >= len(qp.soundList) || qp.soundList[cursor+1] != "a" {
			t.Fatalf("missing sound at slot %d", cursor+1)
		}
	}
}

// TestSoundCatalogBounds rejects both missing extension negotiation and out-of-range slots.
func TestSoundCatalogBounds(t *testing.T) {
	for _, wire := range [][]byte{
		{56, 255, 15, 'a', 0, 0, 0}, // Name beyond slot 4095.
		{56, 0, 1, 'a'},             // Missing string terminator/footer.
	} {
		qp := soundTestProtocol()
		if err := qp.readMessage(wire, mvdMsgRead, playersMaskAll); err == nil {
			t.Fatalf("accepted invalid catalog %v", wire)
		}
	}
	for _, wire := range [][]byte{{56, 0, 1, 0, 0}, {46, 255, 'a', 0, 0, 0}} {
		qp := soundTestProtocol()
		qp.extFlagsFTE1 = 0
		if err := qp.readMessage(wire, mvdMsgRead, playersMaskAll); err == nil {
			t.Fatalf("accepted extended catalog without SOUNDDBL: %v", wire)
		}
	}
}

// TestSoundCatalogQueueSizes checks the default 1024-sound budget and the full-catalog override.
// No socket drains this queue: signon must not depend on the writer goroutine winning a race.
func TestSoundCatalogQueueSizes(t *testing.T) {
	for _, test := range []struct {
		name         string // Scenario label.
		count        int    // Number of sounds with maximum-length MVDSV paths.
		configured   string // Empty selects the default; otherwise overrides the cvar.
		wantOverflow bool   // Whether the queued burst must exceed the selected capacity.
	}{
		{"default-1024", 1024, "", false},
		{"default-4095", 4095, "", true},
		{"increased-4095", 4095, "524288", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			qtv := &QTV{}
			qtv.cmd = newQCmd(qtv)
			qtv.qvs = newQVarStorage(qtv)
			(&dStreamStorage{}).regVars(qtv)
			if test.configured != "" {
				qtv.qvs.Set("dstream_write_buf_size", test.configured)
			}
			ds := soundTestDownstream(qtv.qvs.Get("dstream_write_buf_size").Int, test.count)
			if _, err := ds.wb.WriteFull(make([]byte, 64*1024)); err != nil {
				t.Fatal(err)
			}
			err := ds.linkedUs.qp.sendList(ds, ds.linkedUs.qp.soundList[:], svc_soundlist, protocolSvc(56))
			if test.wantOverflow {
				if err == nil || !strings.Contains(err.Error(), "too many data") {
					t.Fatalf("expected bounded queue overflow, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("sound catalog does not fit selected signon queue: %v", err)
			}
		})
	}
}

// soundTestDownstream uses the production list serializer and ring buffer with no network mocks.
func soundTestDownstream(capacity, count int) *dStream {
	qp := soundTestProtocol()
	us := &uStream{state: usActive, qp: qp, mvdHdr: newNetMsgW(make([]byte, 16), false)}
	qp.us = us
	for i := 1; i <= count; i++ {
		qp.soundList[i] = fmt.Sprintf("sound/%053d.wav", i) // 63 bytes, MVDSV's longest path.
	}
	return &dStream{state: dsNeedSoundList, linkedUs: us, wb: ringbuffer.New(capacity)}
}

// TestSoundCatalogSmallQueueFails explicitly rejects pressure instead of dropping sound chunks.
func TestSoundCatalogSmallQueueFails(t *testing.T) {
	ds := soundTestDownstream(1024, 4095)
	err := ds.linkedUs.qp.sendList(ds, ds.linkedUs.qp.soundList[:], svc_soundlist, protocolSvc(56))
	if err == nil || !strings.Contains(err.Error(), "too many data") {
		t.Fatalf("expected bounded queue failure, got %v", err)
	}
}

// TestSoundQueueOverflowIsIsolated keeps upstream and other viewers alive after a queue fills.
func TestSoundQueueOverflowIsIsolated(t *testing.T) {
	bad := &dStream{id: 1, state: dsActive, wb: ringbuffer.New(128)}
	good := &dStream{id: 2, state: dsActive, wb: ringbuffer.New(4096)}
	us := &uStream{state: usActive, linkedDs: map[dStreamId]*dStream{1: bad, 2: good}}
	payload := bytes.Repeat([]byte{42}, 1024)
	for i := 0; i < 2; i++ {
		if err := us.sendMVDDataDown(payload); err != nil {
			t.Fatal(err)
		}
	}
	if !bad.isCanceled() || good.isCanceled() || us.state != usActive {
		t.Fatal("queue failure did not stay isolated to the overflowing viewer")
	}
	if !bytes.Equal(good.wb.BytesOneReader(), bytes.Repeat(payload, 2)) {
		t.Fatal("healthy viewer lost forwarded bytes")
	}
}

// TestFTEUint64 checks the non-LEB128 length prefix used by extra sound flags.
func TestFTEUint64(t *testing.T) {
	for _, test := range []struct {
		wire []byte // Independent FTE length-prefix encodings.
		want uint64 // Decoded integer, not recomputed by the implementation under test.
	}{
		{[]byte{0}, 0},
		{[]byte{0x7f}, 127},
		{[]byte{0x80, 0x80}, 128},
		{[]byte{0xbf, 0xff}, 16383},
		{[]byte{0xc0, 0x40, 0}, 16384},
		{[]byte{0xff, 0x80, 0, 0, 0, 0, 0, 0, 1}, 0x8000000000000001},
	} {
		reader := newNetMsgR(test.wire, false)
		if value := reader.GetFTEUint64(); value != test.want || reader.error || reader.RPos() != len(test.wire) {
			t.Fatalf("%x: got %x want %x badread=%v", test.wire, value, test.want, reader.error)
		}
		for end := 0; end < len(test.wire); end++ {
			reader.Reset(test.wire[:end], false)
			reader.GetFTEUint64()
			if !reader.error {
				t.Fatalf("accepted truncated FTE integer %x", test.wire[:end])
			}
		}
	}
}

// TestSoundMapReset ensures a new legacy serverdata cannot retain SOUNDDBL or old sounds.
func TestSoundMapReset(t *testing.T) {
	qtv := &QTV{}
	qtv.cmd = newQCmd(qtv)
	qtv.qvs = newQVarStorage(qtv)
	qtv.qvs.Reg("parse_delay", "0")
	qp := soundTestProtocol()
	qp.us = &uStream{qtv: qtv, state: usActive, qp: qp}
	for _, extended := range []bool{false, true, false} {
		wire := []byte{11} // svc_serverdata.
		if extended {
			wire = append(wire, 'F', 'T', 'E', 'X', 128, 0, 0, 0)
		}
		wire = append(wire, 28, 0, 0, 0, 1, 0, 0, 0, 'q', 'w', 0)
		wire = append(wire, 0, 0, 0, 0, 'm', 'a', 'p', 0)
		wire = append(wire, make([]byte, 40)...) // Ten movevars floats.
		if err := qp.readMessage(wire, mvdMsgRead, playersMaskAll); err != nil {
			t.Fatal(err)
		}
		if (qp.extFlagsFTE1&0x80 != 0) != extended || qp.soundList[4095] != "" {
			t.Fatalf("extended=%v: flags=%x stale sound=%q", extended, qp.extFlagsFTE1, qp.soundList[4095])
		}
		qp.soundList[4095] = "previous-map.wav"
	}
}

// TestLateViewerSoundCatalog exercises both production initial/soundlist signon paths.
func TestLateViewerSoundCatalog(t *testing.T) {
	for _, extensions := range []uint32{0, qtvEzQuakeExtDownload} {
		t.Run(fmt.Sprint(extensions), func(t *testing.T) {
			ds := soundTestDownstream(512*1024, 4095)
			ds.state = dsNeedInitialData
			ds.qtvEzQuakeExt = extensions
			us := ds.linkedUs
			us.serverCount = 1
			us.mu.Lock()
			defer us.mu.Unlock()
			if err := us.sendInitialMVDData(ds); err != nil {
				t.Fatal(err)
			}
			if extensions != 0 {
				if ds.state != dsNeedSoundList {
					t.Fatalf("signon state=%v", ds.state)
				}
				command, _ := tokenizeString("qtvsoundlist 1")
				if err := ds.soundListClientCmd(&command); err != nil || ds.state != dsNeedModelList {
					t.Fatalf("soundlist reply: %v state=%v", err, ds.state)
				}
			} else if ds.state != dsActive {
				t.Fatalf("legacy signon state=%v", ds.state)
			}
			// Decode only catalog MVD records independently of the production reader.
			catalog := make([]string, 4096)
			data := ds.wb.BytesOneReader()
			for len(data) != 0 {
				if len(data) < 6 {
					t.Fatal("truncated MVD header")
				}
				size := int(binary.LittleEndian.Uint32(data[2:6]))
				if size > len(data)-6 {
					t.Fatalf("invalid MVD record size %d", size)
				}
				body := data[6 : 6+size]
				data = data[6+size:]
				if len(body) == 0 {
					continue // Initial state may contain an empty MVD record.
				}
				if body[0] != 46 && body[0] != 56 {
					continue
				}
				cursor := int(body[1])
				if body[0] == 56 {
					cursor = int(binary.LittleEndian.Uint16(body[1:3]))
					body = body[3:]
				} else {
					body = body[2:]
				}
				for {
					end := bytes.IndexByte(body, 0)
					if end < 0 {
						t.Fatal("unterminated catalog name")
					}
					if end == 0 {
						if len(body) != 2 {
							t.Fatal("wrong catalog footer size")
						}
						break
					}
					cursor++
					if cursor >= len(catalog) {
						t.Fatal("catalog exceeded supported slots")
					}
					catalog[cursor] = string(body[:end])
					body = body[end+1:]
				}
			}
			for i := 1; i < len(catalog); i++ {
				if catalog[i] != us.qp.soundList[i] {
					t.Fatalf("late viewer slot %d: %q", i, catalog[i])
				}
			}
		})
	}
}
