package qtv

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
)

// readRDFrame consumes one complete updateentities stream into independently owned records.
func readRDFrame(msg *netMsgR, floatCoords bool) (rdFrame, error) {
	return readRDFrameInto(msg, floatCoords, nil)
}

// rdTestFrame wraps literal update records in a complete frame.
func rdTestFrame(records []byte) []byte {
	b := binary.LittleEndian.AppendUint32(nil, math.Float32bits(3))
	b = append(b, records...)
	return append(b, 0, 0)
}

// rdTestPayload reads independent FTE-derived wire bytes, not QTV encoder output.
func rdTestPayload(t *testing.T, width int) []byte {
	t.Helper()
	fixture, err := os.ReadFile(fmt.Sprintf("testdata/replacement_deltas/fte-base-%d.bin", width))
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestRDCodecWire(t *testing.T) {
	for _, width := range []int{2, 4} {
		fixture := rdTestPayload(t, width)
		wire := rdTestFrame(append([]byte{0, 128, 255, 63}, fixture...))
		r := newNetMsgR(wire, width == 4)
		frame, err := readRDFrame(r, width == 4)
		if err != nil || r.RPos() != len(wire) {
			t.Fatalf("width %d: %v consumed %d", width, err, r.RPos())
		}
		w := &rdWorld{}
		if err = w.apply(frame); err != nil {
			t.Fatal(err)
		}
		s := w.current[16383]
		if !w.present[16383] || s.frame != 300 || s.modelIndex != 511 || s.modelIndex2 != 400 || s.skinNum != -1 || s.effects != 0x12345678 || s.weaponFrame != 200 {
			t.Fatalf("lost wide state: %+v", s)
		}
		if s.origin != [3]float32{1.25, -2.5, 3.75} || s.movement != [3]int16{100, -200, 300} || s.velocity != [3]int16{800, -1600, 2400} || s.pmoveType != 3 || s.msec != 14 {
			t.Fatalf("bad movement: %+v", s)
		}
		if math.Abs(float64(s.viewAngles[0]-270)) > .02 || s.tagEntity != 2047 || s.tagIndex != 255 || s.light != [4]uint16{65535, 32768, 1024, 256} || s.trailEffect != 16383 || s.emitEffect != 15 || s.fatness != -5 {
			t.Fatalf("lost auxiliary state: %+v", s)
		}
		if s.trans != 127 || s.scale != 24 || s.drawFlags != 6 || s.absLight != 177 || s.colourMod != [3]byte{1, 2, 3} || s.glowMod != [3]byte{10, 20, 30} || s.gravityDir != [2]byte{12, 34} {
			t.Fatalf("bad visuals: %+v", s)
		}
		for end := 0; end < len(wire); end++ {
			if _, err = readRDFrame(newNetMsgR(wire[:end], width == 4), width == 4); err == nil {
				t.Fatalf("accepted truncated width=%d end=%d", width, end)
			}
		}
	}
}

// Omission, removal and slot reuse must work identically at low and high IDs.
func TestRDWorldRetention(t *testing.T) {
	for _, id := range []uint32{1, 16383} {
		t.Run(fmt.Sprintf("entity%d", id), func(t *testing.T) {
			w := &rdWorld{}
			if err := w.apply(rdFrame{time: 1, updates: []rdUpdate{{number: id}}}); err == nil {
				t.Fatal("accepted delta without reset")
			}
			wire := rdTestFrame([]byte{0, 128, byte(id), byte(id >> 8), 0})
			frame, err := readRDFrame(newNetMsgR(wire, false), false)
			if err != nil {
				t.Fatal(err)
			}
			if err = w.apply(frame); err != nil {
				t.Fatal(err)
			}
			// Independent null-state golden: 96 scales about the entity origin.
			want := rdEntityState{
				number: id, trans: 255, scale: 16, drawFlags: 96,
				colourMod: [3]byte{32, 32, 32}, glowMod: [3]byte{32, 32, 32},
			}
			if !w.ready || !w.present[id] || w.current[id] != want {
				t.Fatalf("bad initial defaults: %+v", w.current[id])
			}
			first := w.current[id]
			if err = w.apply(rdFrame{time: 4}); err != nil || w.current[id] != first || !w.present[id] {
				t.Fatal("omitted entity lost", err)
			}
			w.baselines[id] = defaultRDState()
			w.baselines[id].modelIndex = 9
			w.baselinePresent[id] = true
			if err = w.apply(rdFrame{time: 5, updates: []rdUpdate{{number: id, delta: rdDelta{bits: rdReset}}}}); err != nil || w.current[id].modelIndex != 9 {
				t.Fatal("baseline reset lost", err)
			}
			w.current[id].velocity = [3]int16{8, 16, 24}
			w.current[id].msec = 20
			w.current[id].weaponFrame = 200
			if err = w.apply(rdFrame{time: 6, updates: []rdUpdate{{number: id}}}); err != nil || w.current[id].velocity != [3]int16{} || w.current[id].msec != 0 || w.current[id].weaponFrame != 200 {
				t.Fatal("bad transient rules", err)
			}
			if err = w.apply(rdFrame{time: 7, updates: []rdUpdate{{number: id, remove: true}}}); err != nil || w.present[id] {
				t.Fatal("remove failed", err)
			}
			if err = w.apply(rdFrame{time: 8, updates: []rdUpdate{{number: id}}}); err != nil || !w.present[id] || w.current[id].modelIndex != 0 {
				t.Fatal("reuse retained old state", err)
			}
			if err = w.apply(rdFrame{time: 9, reset: true}); err != nil || w.present[id] || !w.baselinePresent[id] {
				t.Fatal("world reset must preserve baselines", err)
			}
		})
	}
}

func TestRDRejectsIncompleteUpdate(t *testing.T) {
	w := &rdWorld{}
	if err := w.apply(rdFrame{time: 1, reset: true, updates: []rdUpdate{{number: 1}}}); err != nil {
		t.Fatal(err)
	}
	old := append([]rdEntityState(nil), w.current...)
	present := append([]bool(nil), w.present...)
	for _, f := range []rdFrame{{time: float32(math.NaN())}, {time: 2, updates: []rdUpdate{{number: 1, remove: true}, {number: 32768}}}, {time: 2, updates: []rdUpdate{{number: 1}, {number: 1}}}, {time: 2, updates: []rdUpdate{{number: 0}}}} {
		if err := w.apply(f); err == nil || w.time != 1 || !reflect.DeepEqual(old, w.current) || !reflect.DeepEqual(present, w.present) {
			t.Fatal("bad frame mutated accepted world", err)
		}
	}
	for _, wire := range [][]byte{rdTestFrame([]byte{1, 0, 128, 128, 4}), rdTestFrame([]byte{0, 192, 2}), rdTestFrame([]byte{0, 128, 0, 128})} {
		if _, err := readRDFrame(newNetMsgR(wire, false), false); err == nil {
			t.Fatalf("accepted invalid packet %x", wire)
		}
	}
	for _, wire := range [][]byte{{}, {0}, {0, 128}, {255, 255, 255}} {
		if _, err := readRDEntityRef(newNetMsgR(wire, false)); err == nil {
			t.Fatalf("accepted invalid reference %x", wire)
		}
	}
	ref, err := readRDEntityRef(newNetMsgR([]byte{255, 63}, false))
	if err != nil || ref != 16383 {
		t.Fatal(ref, err)
	}
}
