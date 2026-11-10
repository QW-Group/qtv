package qtv

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestRDHighIDRetention(t *testing.T) {
	// Update indices use 14 low bits and an extension byte, unlike ordinary references.
	for _, tc := range []struct {
		id     uint32
		header []byte
	}{
		{16383, []byte{255, 63}},
		{16384, []byte{0, 64, 1}},
		{32767, []byte{255, 127, 1}},
	} {
		t.Run(fmt.Sprint(tc.id), func(t *testing.T) {
			records := append([]byte{0, 128}, tc.header...)
			frame, err := readRDFrame(newNetMsgR(rdTestFrame(append(records, 128, 5, 7)), false), false)
			if err != nil {
				t.Fatal(err)
			}
			w := &rdWorld{}
			if err := w.apply(frame); err != nil || !w.present[tc.id] || w.current[tc.id].number != tc.id || w.current[tc.id].modelIndex != 7 {
				t.Fatal("high-ID update was not retained", err)
			}
			first := w.current[tc.id]
			if err := w.apply(rdFrame{time: 4}); err != nil || !w.present[tc.id] || w.current[tc.id] != first {
				t.Fatal("omitted high ID was lost", err)
			}
			if err := w.apply(rdFrame{time: 5, updates: []rdUpdate{{number: tc.id, remove: true}}}); err != nil || w.present[tc.id] {
				t.Fatal("high-ID removal failed", err)
			}
			want := first
			want.modelIndex = 0
			if err := w.apply(rdFrame{time: 6, updates: []rdUpdate{{number: tc.id}}}); err != nil || !w.present[tc.id] || w.current[tc.id] != want {
				t.Fatal("high-ID reuse failed", err)
			}
		})
	}
}

func TestRDUpdateIndexAtomic(t *testing.T) {
	for _, tc := range []struct {
		id     uint32
		remove bool
		want   []byte
	}{
		{16383, false, []byte{255, 63}},
		{16384, false, []byte{0, 64, 1}},
		{32767, false, []byte{255, 127, 1}},
		{32767, true, []byte{255, 255, 1}},
		{32768, false, nil},
	} {
		t.Run(fmt.Sprintf("id=%d/remove=%v", tc.id, tc.remove), func(t *testing.T) {
			for room := 0; room <= 3; room++ {
				data := []byte{42, 99, 99, 99}
				msg := newNetMsgW(data[:1+room], false)
				msg.PutByte(42)
				msg.PutRDUpdateIndex(tc.id, tc.remove)
				if tc.want == nil || room < len(tc.want) {
					if !msg.error || msg.wPos != 1 || !bytes.Equal(data, []byte{42, 99, 99, 99}) {
						t.Fatalf("partial or invalid index write: room=%d pos=%d bytes=%x", room, msg.wPos, data)
					}
				} else if msg.error || !bytes.Equal(msg.Bytes()[1:], tc.want) {
					t.Fatalf("wrong index: room=%d got=%x want=%x", room, msg.Bytes(), tc.want)
				}
			}
		})
	}
}

func TestRDIndexedStorageGrowth(t *testing.T) {
	q := rdTestProtocol()
	if err := q.readMessage([]byte{21, 0}, mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	if q.rd == nil || len(q.rd.statics) != 1 || len(q.rd.current) != 0 {
		t.Fatal("a static-only map allocated indexed entity state")
	}
	if err := q.readMessage(append([]byte{86}, rdTestFrame([]byte{0, 128, 1, 0, 0})...), mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	if len(q.rd.current) != 2048 {
		t.Fatalf("small world allocated %d slots", len(q.rd.current))
	}
	// Preserve both a live entity and a classic baseline while indexed storage grows.
	baseline := append([]byte{22, 1, 0, 7, 2, 3, 4}, make([]byte, 9)...)
	if err := q.readMessage(baseline, mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	first, base := q.rd.current[1], q.rd.baselines[1]
	for _, id := range []uint32{2048, 4096, 8192, 16384, 32767} {
		if err := q.rd.apply(rdFrame{time: 4, updates: []rdUpdate{{number: id}}}); err != nil {
			t.Fatal(err)
		}
		if !q.rd.present[1] || q.rd.current[1] != first || !q.rd.baselinePresent[1] || !q.rd.baselineClassic[1] || q.rd.baselines[1] != base || len(q.rd.statics) != 1 {
			t.Fatalf("growing for ID %d lost earlier state", id)
		}
		// Every indexed slice must cover the accepted ID without reserving beyond 32K.
		for _, storage := range []struct{ length, capacity int }{
			{len(q.rd.current), cap(q.rd.current)}, {len(q.rd.baselines), cap(q.rd.baselines)},
			{len(q.rd.present), cap(q.rd.present)}, {len(q.rd.baselinePresent), cap(q.rd.baselinePresent)},
			{len(q.rd.baselineClassic), cap(q.rd.baselineClassic)},
		} {
			if storage.length <= int(id) || storage.length != len(q.rd.current) || storage.capacity > 32768 || storage.capacity > 2*(int(id)+1) {
				t.Fatalf("inconsistent storage for ID %d: %+v", id, storage)
			}
		}
	}
	if err := q.rd.apply(rdFrame{time: 5, reset: true}); err != nil || q.rd.present[32767] || !q.rd.baselinePresent[1] || len(q.rd.current) != 32768 {
		t.Fatal("reset lost storage or baselines", err)
	}
	if err := q.readMessage(rdTestServerData(0, 0, 0), mvdMsgRead, playersMaskAll); err != nil || q.rd != nil {
		t.Fatal("map change retained indexed state", err)
	}
	if err := q.readMessage(rdTestServerData(0, ftePext2ReplacementDeltas, 0), mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	if err := q.readMessage(append([]byte{86}, rdTestFrame([]byte{0, 128})...), mvdMsgRead, playersMaskAll); err != nil || len(q.rd.current) != 2048 {
		t.Fatal("new small map retained the large allocation", err)
	}
}

func TestRDInvalidHighIDDoesNotGrowStorage(t *testing.T) {
	q := rdTestProtocol()
	if err := q.readMessage(append([]byte{86}, rdTestFrame([]byte{0, 128, 1, 0, 0})...), mvdMsgRead, playersMaskAll); err != nil {
		t.Fatal(err)
	}
	before := append([]rdEntityState(nil), q.rd.current...)
	present := append([]bool(nil), q.rd.present...)
	if err := q.rd.apply(rdFrame{time: 4, updates: []rdUpdate{{number: 16384}, {number: 32768}}}); err == nil {
		t.Fatal("accepted an out-of-range tail")
	}
	if !reflect.DeepEqual(before, q.rd.current) || !reflect.DeepEqual(present, q.rd.present) || q.rd.time != 3 {
		t.Fatal("an invalid frame grew or changed accepted state")
	}
	// A truncated high-ID baseline must not grow storage before its payload is complete.
	baseline := []byte{66, 255, 127, 128, 5, 7}
	for end := 1; end < len(baseline); end++ {
		if err := q.readMessage(baseline[:end], mvdMsgRead, playersMaskAll); err == nil || len(q.rd.current) != 2048 || len(q.rd.baselines) != 2048 {
			t.Fatalf("truncated baseline grew storage: end=%d err=%v", end, err)
		}
	}
}

func TestRDHighIDLateViewer(t *testing.T) {
	for _, floats := range []bool{false, true} {
		for _, ext := range []uint32{0, qtvEzQuakeExtDownload} {
			t.Run(fmt.Sprintf("float=%v/download=%v", floats, ext != 0), func(t *testing.T) {
				q := rdWriterSource(t, floats)
				// Baselines use ordinary 15-bit references, not extended update indices.
				if err := q.readMessage([]byte{66, 0, 64, 128, 5, 7, 66, 255, 127, 128, 5, 8}, mvdMsgRead, playersMaskAll); err != nil {
					t.Fatal(err)
				}
				if err := q.rd.apply(rdFrame{time: 8, updates: []rdUpdate{
					{number: 16384, delta: rdDelta{bits: rdReset}},
					{number: 32767, delta: rdDelta{bits: rdReset}},
				}}); err != nil {
					t.Fatal(err)
				}
				ds := rdTestViewer(q, ext)
				rdTestSignon(t, ds)
				got, _ := rdTestDecodeSnapshot(t, queueBytes(ds.wb))
				for _, id := range []int{16384, 32767} {
					if !got.rd.present[id] || got.rd.current[id] != q.rd.current[id] || !got.rd.baselinePresent[id] || got.rd.baselines[id] != q.rd.baselines[id] {
						t.Fatalf("late viewer lost state or baseline at ID %d", id)
					}
				}
			})
		}
	}
}
