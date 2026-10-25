package qtv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/markphelps/optional"
	"github.com/qqshka/ringbuffer"
)

// maxPlayersServerData uses literal protocol bytes; the slot byte exists only with bit 0x10.
func maxPlayersServerData(ext2 uint32, slots byte) []byte {
	wire := []byte{11} // svc_serverdata.
	if ext2 != 0 {
		wire = append(wire, 'F', 'T', 'E', '2')
		wire = binary.LittleEndian.AppendUint32(wire, ext2)
	}
	wire = append(wire, 28, 0, 0, 0, 1, 0, 0, 0, 'q', 'w', 0)
	if ext2&0x10 != 0 {
		wire = append(wire, slots)
	}
	wire = append(wire, 0, 0, 0, 0, 'm', 'a', 'p', 0)
	wire = binary.LittleEndian.AppendUint32(wire, math.Float32bits(800)) // Gravity.
	return append(wire, make([]byte, 36)...)                             // Remaining movevars.
}

// maxPlayersUserInfo is a literal svc_updateuserinfo naming the given slot.
func maxPlayersUserInfo(pnum byte) []byte {
	wire := []byte{40, pnum, 1, 0, 0, 0}
	return append(wire, fmt.Sprintf("\\name\\p%d\x00", pnum)...)
}

// TestMaxPlayersServerData checks the slot byte keeps serverdata aligned and bounds player numbers.
func TestMaxPlayersServerData(t *testing.T) {
	qtv := &QTV{}
	qtv.cmd = newQCmd(qtv)
	qtv.qvs = newQVarStorage(qtv)
	qtv.qvs.Reg("parse_delay", "0")
	qp := &qProtocol{r: newNetMsgR(nil, false), w: newNetMsgW(make([]byte, maxMvdSize), false)}
	qp.us = &uStream{qtv: qtv, state: usActive, qp: qp}
	for _, test := range []struct {
		ext2  uint32 // Advertised FTE2 bits; 0x02 alone is what MVDSV sends.
		slots byte   // Announced slot count, ignored without bit 0x10.
		want  int    // Number of player slots the map accepts.
	}{{0, 0, 32}, {0x02, 0, 32}, {0x10, 64, 64}, {0x12, 255, 255}, {0x10, 16, 16}, {0x10, 0, 0}, {0x02, 0, 32}} {
		t.Run(fmt.Sprintf("%x-%d", test.ext2, test.slots), func(t *testing.T) {
			wire := append(maxPlayersServerData(test.ext2, test.slots), 32, 7) // Followed by svc_cdtrack.
			if err := qp.readMessage(wire, mvdMsgRead, playersMaskAll); err != nil {
				t.Fatal(err)
			}
			if qp.mapName != "map" || qp.moveVars.gravity != 800 || qp.cdTrack != 7 {
				t.Fatalf("misaligned serverdata: map=%q gravity=%v cdtrack=%v", qp.mapName, qp.moveVars.gravity, qp.cdTrack)
			}
			if qp.playerSlots() != test.want {
				t.Fatalf("slots=%d want %d", qp.playerSlots(), test.want)
			}
			for pnum := 0; pnum < maxClients; pnum++ {
				if qp.players[pnum].isActive() {
					t.Fatalf("slot %d survived the map reset", pnum)
				}
			}
			for _, pnum := range []int{0, test.want - 1, test.want} {
				if pnum < 0 || pnum > 255 {
					continue
				}
				err := qp.readMessage(maxPlayersUserInfo(byte(pnum)), mvdMsgRead, playersMaskAll)
				if pnum < test.want {
					if err != nil || qp.players[pnum].userInfo.Get("name") != fmt.Sprintf("p%d", pnum) {
						t.Fatalf("slot %d rejected: %v", pnum, err)
					}
				} else if err == nil {
					t.Fatalf("slot %d accepted beyond %d slots", pnum, test.want)
				}
			}
			// svc_updatepl for slot 33: vanilla keeps fteqtv's wrap, the extension does not.
			err := qp.readMessage([]byte{53, 33, 9}, mvdMsgRead, playersMaskAll)
			switch {
			case test.ext2&0x10 == 0:
				if err != nil || qp.players[1].packetLoss != 9 {
					t.Fatalf("vanilla packet loss no longer wraps: %v", err)
				}
			case test.want > 33:
				if err != nil || qp.players[33].packetLoss != 9 || qp.players[1].packetLoss != 0 {
					t.Fatalf("extended packet loss wrapped: %v", err)
				}
			case err == nil:
				t.Fatalf("packet loss accepted beyond %d slots", test.want)
			}
		})
	}
}

// TestMaxPlayersLateViewer decodes signon independently of the production reader.
func TestMaxPlayersLateViewer(t *testing.T) {
	for _, test := range []struct {
		ext2  uint32 // Upstream FTE2 bits; 0x02 alone is what MVDSV sends.
		slots byte   // Announced slot count, ignored without bit 0x10.
		want  int    // Number of userinfo slots a late viewer receives.
	}{{0, 0, 32}, {0x02, 0, 32}, {0x10, 40, 40}, {0x12, 255, 255}} {
		t.Run(fmt.Sprintf("%x-%d", test.ext2, test.slots), func(t *testing.T) {
			qp := &qProtocol{r: newNetMsgR(nil, false), w: newNetMsgW(make([]byte, maxMvdSize), false)}
			us := &uStream{state: usActive, qp: qp, mvdHdr: newNetMsgW(make([]byte, 16), false), serverCount: 1}
			qp.us = us
			ds := &dStream{state: dsActive, linkedUs: us, wb: ringbuffer.New(512 * 1024)}
			qp.gameDir = "qw"
			qp.mapName = "map"
			qp.moveVars.gravity = 800
			qp.baseLine[1].modelIndex = 1 // Catches prespawn offsets that skip into the baselines.
			qp.extFlagsFTE2 = test.ext2
			qp.clientSlots = test.slots
			// Both players have stats, but MVD can only address the first 32 slots.
			for _, pnum := range []int{3, test.want - 1} {
				qp.players[pnum].userInfo.Set("name", fmt.Sprintf("p%d", pnum))
				qp.players[pnum].stats[1] = 100
			}
			for _, send := range []func(*dStream) error{qp.sendServerData, qp.sendPreSpawn, qp.sendPlayerStats} {
				if err := send(ds); err != nil {
					t.Fatal(err)
				}
			}

			var serverData []byte
			var statTargets []byte
			var baseLines []uint16
			userInfos := map[byte]string{}
			data := ds.wb.BytesOneReader()
			for len(data) != 0 {
				if len(data) < 6 {
					t.Fatal("truncated MVD header")
				}
				kind := data[1]
				size := int(binary.LittleEndian.Uint32(data[2:6]))
				if size > len(data)-6 {
					t.Fatalf("invalid MVD record size %d", size)
				}
				body := data[6 : 6+size]
				data = data[6+size:]
				if kind&7 == 5 { // dem_stats.
					statTargets = append(statTargets, kind>>3)
					continue
				}
				if len(body) != 0 && body[0] == 11 {
					serverData = body
					continue
				}
				for len(body) != 0 {
					switch body[0] {
					case 40: // svc_updateuserinfo: slot, userid, string.
						end := 6 + bytes.IndexByte(body[6:], 0)
						userInfos[body[1]] = string(body[6:end])
						body = body[end+1:]
					case 14, 36: // svc_updatefrags, svc_updateping: slot, short.
						body = body[4:]
					case 53: // svc_updatepl: slot, byte.
						body = body[3:]
					case 12: // svc_lightstyle: style, string.
						body = body[2+bytes.IndexByte(body[2:], 0)+1:]
					case 22: // svc_spawnbaseline: entity, four bytes, three fixed coord/angle pairs.
						baseLines = append(baseLines, binary.LittleEndian.Uint16(body[1:3]))
						body = body[16:]
					default:
						t.Fatalf("unexpected prespawn svc %d", body[0])
					}
				}
			}

			if !bytes.HasPrefix(serverData, maxPlayersServerData(test.ext2, test.slots)) {
				t.Fatalf("serverdata layout: %x", serverData)
			}
			if len(userInfos) != test.want {
				t.Fatalf("late viewer got %d userinfo slots, want %d", len(userInfos), test.want)
			}
			for pnum := 0; pnum < test.want; pnum++ {
				if _, ok := userInfos[byte(pnum)]; !ok {
					t.Fatalf("missing userinfo for slot %d", pnum)
				}
			}
			if userInfos[byte(test.want-1)] != fmt.Sprintf("\\name\\p%d", test.want-1) {
				t.Fatalf("last slot userinfo %q", userInfos[byte(test.want-1)])
			}
			if len(baseLines) != 1 || baseLines[0] != 1 {
				t.Fatalf("baselines %v want [1]", baseLines)
			}
			wantTargets := []byte{3}
			if test.want <= 32 {
				wantTargets = append(wantTargets, byte(test.want-1))
			}
			if !bytes.Equal(statTargets, wantTargets) {
				t.Fatalf("stats targets %v want %v", statTargets, wantTargets)
			}
		})
	}
}

// TestMaxPlayersTrackBounds keeps ptrack limited to the slots the upstream announced.
func TestMaxPlayersTrackBounds(t *testing.T) {
	for _, test := range []struct {
		ext2  uint32 // Upstream FTE2 bits; 0x02 alone is what MVDSV sends.
		slots byte   // Announced slot count, ignored without bit 0x10.
		want  int    // Number of slots a viewer may track.
	}{{0x02, 0, 32}, {0x10, 40, 40}} {
		t.Run(fmt.Sprintf("%x-%d", test.ext2, test.slots), func(t *testing.T) {
			qp := &qProtocol{r: newNetMsgR(nil, false), w: newNetMsgW(make([]byte, maxMvdSize), false)}
			us := &uStream{state: usActive, qp: qp, mvdHdr: newNetMsgW(make([]byte, 16), false)}
			qp.us = us
			qp.extFlagsFTE2 = test.ext2
			qp.clientSlots = test.slots
			ds := &dStream{state: dsActive, linkedUs: us, wb: ringbuffer.New(4096)}
			us.mu.Lock()
			defer us.mu.Unlock()
			for _, slot := range []int{test.want - 1, test.want} {
				command, _ := tokenizeString(fmt.Sprintf("ptrack %d", slot))
				if err := ds.ptrackClientCmd(&command); err != nil {
					t.Fatal(err)
				}
				if tracked := ds.pov.Present() && ds.pov.OrElse(-1) == slot; tracked != (slot < test.want) {
					t.Fatalf("ptrack %d with %d slots: tracked=%v", slot, test.want, tracked)
				}
				ds.pov = optional.Int{}
			}
		})
	}
}

// maxPlayersStuffTexts decodes the stufftext payloads queued for a viewer.
func maxPlayersStuffTexts(t *testing.T, ds *dStream) []string {
	var texts []string
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
		if len(body) != 0 && body[0] == 9 { // svc_stufftext.
			texts = append(texts, string(bytes.TrimSuffix(body[1:], []byte{0})))
		}
	}
	return texts
}

// TestMaxPlayersFollowTrack checks followers get the leader's pov for slots past 32.
func TestMaxPlayersFollowTrack(t *testing.T) {
	qp := &qProtocol{r: newNetMsgR(nil, false), w: newNetMsgW(make([]byte, maxMvdSize), false)}
	us := &uStream{state: usActive, qp: qp, mvdHdr: newNetMsgW(make([]byte, 16), false)}
	qp.us = us
	qp.extFlagsFTE2 = ftePext2MaxPlayers
	qp.clientSlots = 40
	qp.players[39].userId = 1234
	qp.players[39].userInfo.Set("name", "p39")
	leader := &dStream{id: 1, state: dsActive, linkedUs: us, wb: ringbuffer.New(4096)}
	early := &dStream{id: 2, state: dsActive, linkedUs: us, wb: ringbuffer.New(4096), followId: 1}
	late := &dStream{id: 3, state: dsActive, linkedUs: us, wb: ringbuffer.New(4096)}
	us.linkedDs = map[dStreamId]*dStream{1: leader, 2: early, 3: late}
	us.mu.Lock()
	defer us.mu.Unlock()

	// The early follower sees the change, the late one gets it on follow, a rejected slot sends nothing.
	for _, step := range []struct {
		ds   *dStream // Viewer issuing the command.
		line string   // Client command.
	}{{leader, "ptrack 39"}, {late, "follow 1"}, {leader, "ptrack 40"}} {
		command, _ := tokenizeString(step.line)
		var err error
		if step.ds == late {
			err = step.ds.followClientCmd(&command)
		} else {
			err = step.ds.ptrackClientCmd(&command)
		}
		if err != nil {
			t.Fatalf("%s: %v", step.line, err)
		}
	}
	for _, follower := range []*dStream{early, late} {
		if texts := maxPlayersStuffTexts(t, follower); len(texts) != 1 || texts[0] != "track 1234\n" {
			t.Fatalf("follower %d got %q, want one track for slot 39", follower.id, texts)
		}
	}
	if texts := maxPlayersStuffTexts(t, leader); len(texts) != 0 || leader.pov.OrElse(-1) != 39 {
		t.Fatalf("leader pov=%v texts=%q", leader.pov.OrElse(-1), texts)
	}
}
