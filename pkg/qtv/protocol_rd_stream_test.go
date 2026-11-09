package qtv

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

// rdTestProtocol uses the real protocol and source without network workers.
func rdTestProtocol() *qProtocol {
	q := queueTestQTV()
	q.qvs.Set("parse_delay", "0")
	us := &uStream{qtv: q, state: usActive}
	us.qp = newQProtocol(us)
	us.qp.extFlagsFTE2 = ftePext2ReplacementDeltas
	return us.qp
}

// rdTestServerData supplies literal capability headers and QW serverdata.
func rdTestServerData(bits1, bits2, mvd uint32) []byte {
	b := []byte{11}
	for _, p := range []struct {
		id   string
		bits uint32
	}{{"FTEX", bits1}, {"FTE2", bits2}, {"MVD1", mvd}} {
		if p.bits != 0 {
			b = append(b, p.id...)
			b = binary.LittleEndian.AppendUint32(b, p.bits)
		}
	}
	b = binary.LittleEndian.AppendUint32(b, 28)
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = append(b, 'q', 'w', 0)
	b = binary.LittleEndian.AppendUint32(b, 0)
	b = append(b, 'm', 'a', 'p', 0)
	return append(b, make([]byte, 40)...)
}

func TestRDSourceMessages(t *testing.T) {
	qp := rdTestProtocol()
	// Full ordinary reference, UF_RESET|UF_MODEL, model7; static model8; marker.
	b := []byte{66, 255, 63, 128, 5, 7, 21, 128, 4, 8, 32, 9}
	if err := qp.readMessage(b, mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	if qp.rd == nil || qp.rd.baselines[16383].modelIndex != 7 || len(qp.rd.statics) != 1 || qp.rd.statics[0].modelIndex != 8 || qp.cdTrack != 9 {
		t.Fatal("lost signon state")
	}
	b = append([]byte{86}, rdTestFrame([]byte{0, 128, 255, 63, 128, 1})...)
	b = append(b, 32, 10)
	if err := qp.readMessage(b, mvdMsgRead, playersMaskAll); err != nil || !qp.rd.present[16383] || qp.rd.current[16383].modelIndex != 7 || qp.cdTrack != 10 {
		t.Fatal("lost retained state", err)
	}
	// Entity-only float coordinates must not widen sound/other event coordinates or angles.
	// Fixed wire capability bits also catch wrong RD/MVD constant definitions.
	if err := qp.readMessage(rdTestServerData(0, 0x00000008, 0x00000001), mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	b = []byte{86}
	f := rdTestFrame([]byte{0, 128, 1, 0, 2})
	f = f[:len(f)-2]
	f = binary.LittleEndian.AppendUint32(f, math.Float32bits(1.25))
	f = binary.LittleEndian.AppendUint32(f, math.Float32bits(-2.5))
	f = append(f, 0, 0)
	b = append(b, f...)
	b = append(b, 32, 11)
	if err := qp.readMessage(b, mvdMsgRead, playersMaskAll); err != nil || qp.rd.current[1].origin[0] != 1.25 || qp.cdTrack != 11 {
		t.Fatal("wrong entity-only coords", err)
	}
	// Ordinary legacy baselines remain accepted under RD, with RD defaults.
	b = []byte{22, 1, 0, 7, 2, 3, 4}
	b = append(b, make([]byte, 9)...)
	b = append(b, 32, 12)
	if err := qp.readMessage(b, mvdMsgRead, playersMaskAll); err != nil || qp.rd.baselines[1].modelIndex != 7 || qp.rd.baselines[1].trans != 255 || qp.cdTrack != 12 {
		t.Fatal("legacy baseline in RD", err)
	}
	baseline := []byte{66, 255, 63, 128, 5, 7}
	for end := 1; end < len(baseline); end++ {
		q := rdTestProtocol()
		if err := q.readMessage(baseline[:end], mvdMsgRead, playersMaskAll); err == nil || q.rd != nil && q.rd.baselinePresent[16383] {
			t.Fatal("truncated baseline published")
		}
	}
}

func TestRDSourceMessagesRejectInvalid(t *testing.T) {
	q := rdTestProtocol()
	q.extFlagsFTE1 = ftePextFloatCoords
	b := []byte{22, 1, 0, 7, 2, 3, 4}
	for _, v := range []float32{1.25, -2.5, 3.75} {
		b = binary.LittleEndian.AppendUint32(b, math.Float32bits(v))
		b = append(b, 0, 0)
	}
	b = append(b, 32, 47)
	if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil || q.rd.baselines[1].origin != [3]float32{1.25, -2.5, 3.75} || q.cdTrack != 47 {
		t.Fatal("legacy baseline lost ordinary float coordinates", err)
	}
	q = rdTestProtocol()
	// UF_EXTEND1|UF_RESET|UF_WIDE|UF_MODEL, resource 65535.
	b = append([]byte{86}, rdTestFrame([]byte{0, 128, 1, 0, 128, 7, 255, 255})...)
	if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err == nil {
		t.Fatal("accepted an entity model outside the unchanged resource capacity")
	}
	q = rdTestProtocol()
	q.extFlagsFTE1 = ftePextFloatCoords
	b = []byte{22, 1, 0, 7, 2, 3, 4}
	b = binary.LittleEndian.AppendUint32(b, math.Float32bits(float32(math.NaN())))
	b = append(b, make([]byte, 14)...)
	if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err == nil || q.rd != nil && q.rd.baselinePresent[1] {
		t.Fatal("accepted nonfinite legacy baseline")
	}
}

func TestRDSourceClassicSignonWithEntityOnlyFloats(t *testing.T) {
	for _, opcode := range []byte{20, 22} {
		q := rdTestProtocol()
		q.extFlagsMVD1 = mvdPext1FloatCoords
		b := []byte{opcode}
		if opcode == byte(svc_spawnbaseline) {
			b = append(b, 1, 0)
		}
		b = append(b, 7, 2, 3, 4)
		for _, v := range []int16{8, -16, 24} {
			b = binary.LittleEndian.AppendUint16(b, uint16(v))
			b = append(b, 0)
		}
		b = append(b, 32, 46)
		if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil || q.cdTrack != 46 {
			t.Fatal("classic signon consumed entity-only floats", opcode, err)
		}
		s := q.rd.baselines[1]
		if opcode == byte(svc_spawnstatic) {
			s = q.rd.statics[0]
		}
		if s.origin != [3]float32{1, -2, 3} {
			t.Fatal("wrong classic coordinate width", s.origin)
		}
	}
}

func TestRDEntityEvents(t *testing.T) {
	for _, id := range []uint16{2047, 2048, 4095, 4096, 8191, 8192, 16383} {
		for _, flags := range []uint32{0, ftePextSoundDbl, ftePextFloatCoords, ftePextSoundDbl | ftePextFloatCoords} {
			q := rdTestProtocol()
			q.extFlagsFTE1 = flags
			size := 6
			if flags&ftePextFloatCoords != 0 {
				size = 12
			}
			coord := make([]byte, size)
			for _, sample := range []byte{0, 1} {
				b := []byte{55, 8}
				b = binary.LittleEndian.AppendUint16(b, id)
				b = append(b, 3, sample)
				b = append(b, coord...)
				b = append(b, 32, 42)
				if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil || q.cdTrack != 42 {
					t.Fatalf("sound id=%d flags=%x: %v", id, flags, err)
				}
			}
			for _, b := range [][]byte{{39, byte(id), byte(id >> 8)}, {23, 5, byte(id), byte(id >> 8)}} {
				if b[0] == byte(svc_temp_entity) {
					b = append(b, make([]byte, size*2)...)
				}
				b = append(b, 32, 43)
				if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil || q.cdTrack != 43 {
					t.Fatal("entity event lost framing", err)
				}
			}
			b := []byte{56, 0, 1, 'a', 0, 0, 0, 32, 44}
			if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil || q.soundList[257] != "a" || q.cdTrack != 44 {
				t.Fatal("RD-implied sound catalog", err)
			}
			b = []byte{60, 0, 1, 'm', 0, 0, 0, 32, 45}
			if err := q.readMessage(b, mvdMsgRead, playersMaskAll); err != nil || q.modelList[257] != "m" || q.cdTrack != 45 {
				t.Fatal("RD-implied model catalog", err)
			}
		}
	}
	q := rdTestProtocol()
	if err := q.readMessage([]byte{39, 0, 64}, mvdMsgRead, playersMaskAll); err == nil {
		t.Fatal("accepted out-of-capacity event reference")
	}
}

func TestRDProfile(t *testing.T) {
	// QTV rejects FTE2 bits outside the supported base-RD/voice profile.
	for _, unsupported := range []uint32{1 << 5, 1 << 6, 1 << 7, 1 << 8, 1 << 9} {
		bits := ftePext2ReplacementDeltas | unsupported
		q := rdTestProtocol()
		if err := q.readMessage(rdTestServerData(0, bits, 0), mvdMsgRead, playersMaskAll); err == nil {
			t.Fatalf("accepted unsupported profile %x", bits)
		}
	}
	q := rdTestProtocol()
	if err := q.readMessage(rdTestServerData(ftePextCSQC, ftePext2ReplacementDeltas, 0), mvdMsgRead, playersMaskAll); err == nil {
		t.Fatal("accepted CSQC")
	}
	// Published RD and RD+voice bits stay literal in this header golden.
	for _, wireBits := range []uint32{0x00000008, 0x0000000a} {
		q := rdTestProtocol()
		q.extFlagsFTE2 = 0
		if err := q.readMessage(rdTestServerData(0, wireBits, 0), mvdMsgRead, playersMaskAll); err != nil || !q.usesRD() {
			t.Fatalf("wire profile %x did not select RD: %v", wireBits, err)
		}
	}
}

// rdTestMVD wraps literal service bytes in a broadcast MVD record.
func rdTestMVD(payload []byte) []byte {
	b := []byte{0, 6}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, payload...)
}

func TestRDSourceFraming(t *testing.T) {
	good := rdTestMVD(append([]byte{86}, rdTestFrame([]byte{0, 128, 1, 0, 0})...))
	for end := 0; end < len(good); end++ {
		q := rdTestProtocol()
		done, discard, _, err := q.us.parseMVDMessage(good[:end], 1, true)
		if !done || discard != 0 || err != nil || q.rd != nil {
			t.Fatal("partial MVD was consumed", end, err)
		}
	}
	q := rdTestProtocol()
	q.us.rb = mustByteQueue(4096, 64*1024)
	q.us.io = &uStreamFile{}
	q.us.curTime = 1
	bad := rdTestMVD(append([]byte{86}, rdTestFrame([]byte{0, 128, 1, 0, 0, 255, 63, 128})...))
	viewer := &dStream{id: 1, state: dsActive, wb: mustByteQueue(4096, 16*1024)}
	q.us.linkedDs = map[dStreamId]*dStream{1: viewer}
	if _, err := q.us.rb.WriteFull(bad); err != nil {
		t.Fatal(err)
	}
	if err := q.us.parseMVD(); err == nil || len(queueBytes(viewer.wb)) != 0 {
		t.Fatal("forwarded malformed RD message", err)
	}
	q = rdTestProtocol()
	q.us.rb = mustByteQueue(4096, 64*1024)
	q.us.io = &uStreamFile{}
	q.us.curTime = 1
	q.us.linkedDs = map[dStreamId]*dStream{1: viewer}
	if _, err := q.us.rb.WriteFull(good); err != nil {
		t.Fatal(err)
	}
	if err := q.us.parseMVD(); err != nil || !bytes.Equal(queueBytes(viewer.wb), good) || !q.rd.present[1] {
		t.Fatal("failed accepted forwarding", err)
	}
}
