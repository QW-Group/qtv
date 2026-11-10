package qtv

import (
	"bytes"
	"fmt"
	"testing"
)

// Wire fixture codes and payload bytes stay literal to catch wrong protocol constants.
// fteTempEntityWire supplies independently encoded fixed/float start and end coordinates.
func fteTempEntityWire(kind byte, ref []byte, floats bool) []byte {
	wire := append([]byte{23, kind}, ref...)
	if floats {
		// 1.25, -2.5, 3.75, -5, 6.25, -7.5 as IEEE little-endian floats.
		return append(wire, 0, 0, 0xa0, 0x3f, 0, 0, 0x20, 0xc0, 0, 0, 0x70, 0x40,
			0, 0, 0xa0, 0xc0, 0, 0, 0xc8, 0x40, 0, 0, 0xf0, 0xc0)
	}
	return append(wire, 10, 0, 0xec, 0xff, 30, 0, 0xd8, 0xff, 50, 0, 0xc4, 0xff)
}

// TestFTETempEntityLayouts catches missing event layouts, wrong RD references and svc misalignment.
func TestFTETempEntityLayouts(t *testing.T) {
	for _, floats := range []bool{false, true} {
		for _, rd := range []bool{false, true} {
			for _, effect := range []struct {
				name      string
				wireType  byte
				reference bool
			}{
				{"lightning1", 5, true}, {"lightning2", 6, true},
				{"lightning3", 9, true}, {"railtrail", 17, false}, {"beam", 18, true},
			} {
				kind := effect.wireType
				refs := [][]byte{{0, 0}, {1, 0}, {0xff, 0x3f}}
				if !effect.reference {
					refs = [][]byte{nil}
				} else if rd {
					// FTE ordinary keys span 15 direct bits or 23 extended bits.
					refs = append(refs, []byte{1, 0x80, 0}, []byte{0x3f, 0x80, 0xff},
						[]byte{0, 0x40}, []byte{0xff, 0x7f}, []byte{0x80, 0x80, 0},
						[]byte{0, 0x90, 0}, []byte{0xff, 0xff, 0xff})
				} else {
					// Raw legacy 16-bit values include signed lightning aliases.
					refs = append(refs, []byte{0xff, 0xff}, []byte{0, 0xfe}, []byte{0xf9, 0xfa})
				}
				for _, ref := range refs {
					t.Run(fmt.Sprintf("float=%v/RD=%v/%s/type=%d/ref=%x", floats, rd, effect.name, kind, ref), func(t *testing.T) {
						q := rdTestProtocol()
						q.extFlagsFTE2 = 0
						if rd {
							q.extFlagsFTE2 = ftePext2ReplacementDeltas
						}
						if floats {
							q.extFlagsFTE1 = ftePextFloatCoords
						}
						wire := fteTempEntityWire(kind, ref, floats)
						packet := append(append([]byte(nil), wire...), 32, 77)
						original := append([]byte(nil), packet...)
						if err := q.readMessage(packet, mvdMsgRead, playersMaskAll); err != nil || q.cdTrack != 77 {
							t.Fatalf("event parse: %v, following svc marker=%d", err, q.cdTrack)
						}
						if !bytes.Equal(packet, original) || q.rd != nil {
							t.Fatal("transient event changed packet bytes or allocated retained RD state")
						}
						for end := 1; end < len(wire); end++ {
							if err := q.readMessage(wire[:end], mvdMsgRead, playersMaskAll); err == nil {
								t.Errorf("accepted truncated event at byte %d/%d", end, len(wire))
							}
						}
					})
				}
			}
		}
	}
}

// TestFTETempEntityWorldLimit stays strict for retained references despite unrestricted beam keys.
func TestFTETempEntityWorldLimit(t *testing.T) {
	for _, ref := range [][]byte{{0x80, 0x80, 0}, {0x80, 0x80, 1}, {0xff, 0xff, 0xff}} {
		if _, err := readRDEntityRef(newNetMsgR(ref, false)); err == nil {
			t.Errorf("accepted out-of-range retained RD ref: %x", ref)
		}
		q := rdTestProtocol()
		// svc_soundextended, large entity, then ordinary entity reference.
		wire := append([]byte{55, 8}, ref...)
		wire = append(wire, 1, 1, 0, 0, 0, 0, 0, 0) // channel, sound and three fixed coords.
		if err := q.readMessage(wire, mvdMsgRead, playersMaskAll); err == nil {
			t.Errorf("accepted out-of-range sound entity ref: %x", ref)
		}
	}
}

// TestFTETempEntityMapProfiles catches stale RD/coordinate negotiation across map changes.
func TestFTETempEntityMapProfiles(t *testing.T) {
	q := rdTestProtocol()
	for _, floats := range []bool{false, true} {
		for _, rd := range []bool{false, true, false} {
			bits1, bits2 := uint32(0), uint32(0)
			if floats {
				bits1 = ftePextFloatCoords
			}
			if rd {
				bits2 = ftePext2ReplacementDeltas
			}
			if err := q.readMessage(rdTestServerData(bits1, bits2, 0), mvdMsgRead, playersMaskAll); err != nil {
				t.Fatal(err)
			}
			for _, kind := range []byte{teLightning1, teLightning2, teLightning3, teRailTrail, teBeam} {
				ref := []byte{0xff, 0xff}
				if rd {
					ref = []byte{0xff, 0xff, 0xff}
				}
				if kind == teRailTrail {
					ref = nil
				}
				wire := append(fteTempEntityWire(kind, ref, floats), 32, 77)
				if err := q.readMessage(wire, mvdMsgRead, playersMaskAll); err != nil || q.cdTrack != 77 || q.rd != nil {
					t.Fatalf("map event RD=%v float=%v type=%d: err=%v marker=%d", rd, floats, kind, err, q.cdTrack)
				}
			}
		}
	}
}

// TestFTETempEntityForwarding catches loss or rewriting of transient MVD events.
func TestFTETempEntityForwarding(t *testing.T) {
	for _, floats := range []bool{false, true} {
		q := rdTestProtocol()
		if floats {
			q.extFlagsFTE1 = ftePextFloatCoords
		}
		q.us.rb = mustByteQueue(4096, 64*1024)
		q.us.io = &uStreamFile{}
		q.us.curTime = 1
		viewer := &dStream{id: 1, state: dsActive, wb: mustByteQueue(4096, 16*1024)}
		q.us.linkedDs = map[dStreamId]*dStream{1: viewer}
		var payload []byte
		for _, kind := range []byte{teLightning1, teLightning2, teLightning3, teRailTrail, teBeam} {
			ref := []byte{0xff, 0xff, 0xff}
			if kind == teRailTrail {
				ref = nil
			}
			payload = append(payload, fteTempEntityWire(kind, ref, floats)...)
		}
		payload = append(payload, 32, 77)
		wire := rdTestMVD(payload)
		if _, err := q.us.rb.WriteFull(wire); err != nil {
			t.Fatal(err)
		}
		if err := q.us.parseMVD(); err != nil || !bytes.Equal(queueBytes(viewer.wb), wire) || q.cdTrack != 77 || q.rd != nil {
			t.Fatalf("MVD event forwarding float=%v: err=%v marker=%d", floats, err, q.cdTrack)
		}
	}
}

// ftePositionEventWire is a byte golden independent of the production writer.
func ftePositionEventWire(kind byte, floats bool, palette []byte) []byte {
	wire := []byte{23, kind}
	if floats {
		// 1.25, -2.5, 3.75, IEEE little-endian floats.
		wire = append(wire, 0, 0, 0xa0, 0x3f, 0, 0, 0x20, 0xc0, 0, 0, 0x70, 0x40)
	} else {
		// The same position as signed shorts scaled by 1/8.
		wire = append(wire, 10, 0, 0xec, 0xff, 30, 0)
	}
	return append(wire, palette...)
}

func TestFTEPositionEventLayouts(t *testing.T) {
	for _, bits1 := range []uint32{0, ftePextFloatCoords} {
		for _, bits2 := range []uint32{0, ftePext2ReplacementDeltas} {
			for _, mvd := range []uint32{0, mvdPext1FloatCoords} {
				for _, effect := range []struct {
					name     string
					wireType byte
					palette  bool
				}{
					{"bullet", 14, false}, {"superbullet", 15, false},
					{"explosion2", 19, true}, {"nq-explosion", 20, false}, {"nq-gunshot", 21, false},
				} {
					kind := effect.wireType
					palettes := [][]byte{nil}
					if effect.palette {
						palettes = [][]byte{{0, 0}, {255, 0}, {0, 255}, {255, 255}, {255, 1}, {254, 2}}
					}
					for _, palette := range palettes {
						t.Run(fmt.Sprintf("FTE1=%x/FTE2=%x/MVD1=%x/%s/type=%d/palette=%x", bits1, bits2, mvd, effect.name, kind, palette), func(t *testing.T) {
							q := rdTestProtocol()
							if err := q.readMessage(rdTestServerData(bits1, bits2, mvd), mvdMsgRead, playersMaskAll); err != nil {
								t.Fatal(err)
							}
							wire := ftePositionEventWire(kind, bits1 != 0, palette)
							// No padding: consuming one extra byte must break the following svc.
							packet := append(append([]byte(nil), wire...), 32, 77)
							original := append([]byte(nil), packet...)
							if err := q.readMessage(packet, mvdMsgRead, playersMaskAll); err != nil || q.cdTrack != 77 {
								t.Fatalf("event parse: %v, marker=%d", err, q.cdTrack)
							}
							if !bytes.Equal(packet, original) || q.rd != nil {
								t.Fatal("changed raw event bytes or allocated retained state")
							}
							for end := 1; end < len(wire); end++ {
								if err := q.readMessage(wire[:end], mvdMsgRead, playersMaskAll); err == nil {
									t.Errorf("accepted truncated event at %d/%d", end, len(wire))
								}
								q.us.rb = mustByteQueue(4096, 64*1024)
								q.us.io = &uStreamFile{}
								q.us.parseTime, q.us.curTime = 0, 1
								viewer := &dStream{id: 1, state: dsActive, wb: mustByteQueue(4096, 16*1024)}
								q.us.linkedDs = map[dStreamId]*dStream{1: viewer}
								if _, err := q.us.rb.WriteFull(rdTestMVD(wire[:end])); err != nil {
									t.Fatal(err)
								}
								if err := q.us.parseMVD(); err == nil || viewer.wb.Length() != 0 {
									t.Errorf("accepted or forwarded truncated MVD event at %d/%d: %v", end, len(wire), err)
								}
							}
						})
					}
				}
			}
		}
	}
}

func TestFTEPositionEventMapResetAndForwarding(t *testing.T) {
	q := rdTestProtocol()
	q.us.wb = mustByteQueue(4096, 4*1024)
	for _, profile := range [][3]uint32{
		{0, 0, 0},
		{ftePextFloatCoords, ftePext2ReplacementDeltas, 0},
		{0, ftePext2ReplacementDeltas, mvdPext1FloatCoords},
		{ftePextFloatCoords, 0, mvdPext1FloatCoords},
		{0, 0, 0},
	} {
		if err := q.readMessage(rdTestServerData(profile[0], profile[1], profile[2]), mvdMsgRead, playersMaskAll); err != nil {
			t.Fatal(err)
		}
		q.us.state = usActive // Complete map signon before exercising active forwarding.
		q.us.parseTime = 0
		q.us.rb = mustByteQueue(4096, 64*1024)
		q.us.io = &uStreamFile{}
		q.us.curTime = 1
		viewer := &dStream{id: 1, state: dsActive, wb: mustByteQueue(4096, 16*1024)}
		q.us.linkedDs = map[dStreamId]*dStream{1: viewer}
		var payload []byte
		for _, kind := range []byte{teBullet, teSuperBullet, teExplosion2, teNQExplosion, teNQGunShot} {
			var palette []byte
			if kind == teExplosion2 {
				palette = []byte{255, 0}
			}
			payload = append(payload, ftePositionEventWire(kind, profile[0] != 0, palette)...)
			payload = append(payload, byte(svc_nop)) // Preserve an interleaved ordinary svc when forwarding.
		}
		wire := rdTestMVD(append(payload, 32, 78))
		if _, err := q.us.rb.WriteFull(wire); err != nil {
			t.Fatal(err)
		}
		if err := q.us.parseMVD(); err != nil || !bytes.Equal(queueBytes(viewer.wb), wire) || q.cdTrack != 78 || q.rd != nil {
			t.Fatalf("raw MVD forwarding profile=%x: err=%v marker=%d", profile, err, q.cdTrack)
		}
	}
}

func TestFTEPositionEventLegacyAndNQPolicy(t *testing.T) {
	for _, floats := range []bool{false, true} {
		q := rdTestProtocol()
		if floats {
			q.extFlagsFTE1 = ftePextFloatCoords
		}
		for _, effect := range []struct {
			name     string
			wireType byte
			count    bool
		}{
			{"spike", 0, false}, {"superspike", 1, false}, {"gunshot", 2, true},
			{"explosion", 3, false}, {"tar-explosion", 4, false},
			{"wizspike", 7, false}, {"knightspike", 8, false},
			{"lava-splash", 10, false}, {"teleport", 11, false},
			{"blood", 12, true}, {"lightning-blood", 13, false},
		} {
			wire := ftePositionEventWire(effect.wireType, floats, nil)
			if effect.count {
				// Legacy QW gunshot and blood still have a count before position.
				wire = append([]byte{23, effect.wireType, 255}, wire[2:]...)
			}
			if err := q.readMessage(append(wire, 1, 32, 79), mvdMsgRead, playersMaskAll); err != nil || q.cdTrack != 79 {
				t.Fatalf("legacy %s type=%d float=%v: %v marker=%d", effect.name, effect.wireType, floats, err, q.cdTrack)
			}
		}
	}
	q := rdTestProtocol()
	nq := rdTestServerData(0, 0, 0)
	nq[1] = 15 // NetQuake protocol is still unsupported by QTV's QW reader.
	if err := q.readMessage(nq, mvdMsgRead, playersMaskAll); err == nil {
		t.Fatal("accepted NetQuake serverdata")
	}
}
