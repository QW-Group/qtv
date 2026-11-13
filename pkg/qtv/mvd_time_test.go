package qtv

import (
	"bytes"
	"math"
	"testing"
)

func TestMVDRecordPayload(t *testing.T) {
	for _, tc := range []struct {
		name   string
		wire   []byte
		hidden bool
		kind   mvdMsgType
		mask   uint32
		want   []byte
	}{
		{"read", []byte{0, 1, 0, 0, 0, 0}, false, mvdMsgRead, playersMaskAll, []byte{}},
		{"all", []byte{0, 6, 2, 0, 0, 0, 32, 9}, false, mvdMsgAll, playersMaskAll, []byte{32, 9}},
		{"single", []byte{0, 4 | (31 << 3), 2, 0, 0, 0, 32, 9}, false, mvdMsgSingle, 1 << 31, []byte{32, 9}},
		{"stats", []byte{0, 5 | (7 << 3), 2, 0, 0, 0, 32, 9}, false, mvdMsgStats, 1 << 7, []byte{32, 9}},
		{"multi", []byte{0, 3, 3, 0, 0, 128, 2, 0, 0, 0, 32, 9}, true, mvdMsgMulti, 1<<31 | 3, []byte{32, 9}},
		{"legacy zero mask", []byte{0, 3, 0, 0, 0, 0, 2, 0, 0, 0, 32, 9}, false, mvdMsgMulti, 0, []byte{32, 9}},
		{"hidden zero mask", []byte{0, 3, 0, 0, 0, 0, 2, 0, 0, 0, 32, 9}, true, mvdMsgMulti, 0, nil},
		{"set", []byte{0, 2, 1, 0, 0, 0, 2, 0, 0, 0}, false, mvdMsgSet, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Deliberately split the header and include bytes of the next record.
			input := append(bytes.Clone(tc.wire), 7, 6, 0, 0, 0, 0)
			view := byteView{parts: [][]byte{input[:3], input[3:]}, size: len(input)}
			cursor := view.cursor(0)
			header, status := inspectMVDRecord(&cursor, false)
			if status != mvdRecordComplete || header.size != len(tc.wire) || header.kind != tc.kind || header.playersMask != tc.mask {
				t.Fatalf("header=%+v status=%v", header, status)
			}
			got := header.quakePayload(input, tc.hidden)
			if (got == nil) != (tc.want == nil) || !bytes.Equal(got, tc.want) {
				t.Fatalf("payload=%#v want=%#v", got, tc.want)
			}
		})
	}
}

// A stale cached prefix or subtraction of normalized timestamps changes these sums.
func TestMVDTimeAppendConsumeAndFlags(t *testing.T) {
	first := []byte{13, 6, 2, 0, 0, 0, 32, 7}
	next := []byte{7, 4, 0, 0, 0, 0}
	raw := append(append([]byte{}, first...), next...)
	var c mvdTimeCache
	if got := c.pending(byteViewOf(raw[:len(raw)-1]), false); got != 13 {
		t.Fatal(got)
	}
	if got := c.pending(byteViewOf(raw), false); got != 20 {
		t.Fatal(got)
	}
	if got := c.pending(byteViewOf(raw), false); got != 20 {
		t.Fatal("double count", got)
	}
	raw[0] = 200
	c.consume(len(first), 13, false)
	if got := c.pending(byteViewOf(raw[len(first):]), false); got != 7 {
		t.Fatal("normalized debit", got)
	}
	hidden := []byte{0, 3, 0, 0, 0, 0, 3, 0, 0, 0, 10, 0, 42}
	c.reset()
	if c.pending(byteViewOf(hidden), false) != 0 || c.pending(byteViewOf(hidden), true) != 42 || c.pending(byteViewOf(hidden), false) != 0 {
		t.Fatal("hidden interpretation")
	}
	c.reset()
	if c.pending(byteViewOf(nil), true) != 0 {
		t.Fatal("reset")
	}
}

// Valid record totals can exceed int32 without requiring a large test allocation.
func TestMVDTimeAboveInt32(t *testing.T) {
	records := int64(math.MaxInt32)/255 + 1
	total := records * 255
	c := mvdTimeCache{valid: true, bytes: int(records * 6), milliseconds: total}
	c.consume(6, 255, false)
	if c.milliseconds != total-255 || c.bytes != int(records*6)-6 {
		t.Fatal(c)
	}
}

// Every header split and paused/set/directed layout must match literal container rules.
func TestMVDTimeFraming(t *testing.T) {
	cases := []struct {
		wire    []byte
		elapsed bool
		size    int
		ms      byte
	}{
		{[]byte{9, 2, 0, 0, 0, 0, 0, 0, 0, 0}, false, 10, 9},
		{[]byte{7, 6, 0, 0, 0, 0}, false, 6, 7},
		{[]byte{8, 4, 0, 0, 0, 0}, false, 6, 8},
		{[]byte{8, 5, 0, 0, 0, 0}, false, 6, 8},
		{[]byte{0, 3, 0, 0, 0, 0, 3, 0, 0, 0, 10, 0, 42}, true, 13, 42},
	}
	for _, tc := range cases {
		for cut := 0; cut <= len(tc.wire); cut++ {
			// Make the header straddle two separately owned byte spans.
			mid := cut / 2
			v := byteView{parts: [][]byte{tc.wire[:mid], tc.wire[mid:cut]}, size: cut}
			cursor := v.cursor(0)
			header, state := inspectMVDRecord(&cursor, tc.elapsed)
			if cut < len(tc.wire) {
				if state != mvdRecordIncomplete {
					t.Fatalf("cut%d: %d", cut, state)
				}
			} else if state != mvdRecordComplete || header.size != tc.size || header.playbackMS != tc.ms {
				t.Fatalf("header=%+v state=%d", header, state)
			}
		}
	}
	for _, bad := range [][]byte{{0, 7}, {0, 6, 255, 255, 255, 255}} {
		cur := byteViewOf(bad).cursor(0)
		_, state := inspectMVDRecord(&cur, false)
		if state != mvdRecordInvalid {
			t.Fatal("bad accepted")
		}
	}
}

// An svc error must not debit bytes that remain queued for source teardown.
func TestMVDTimeErrorBeforeDiscard(t *testing.T) {
	us, _ := bufferingTestStream(t, 7, false)
	good := []byte{20, 6, 2, 0, 0, 0, 32, 7}
	bad := []byte{0, 6, 1, 0, 0, 0, 255}
	us.rb.WriteFull(append(append([]byte{}, good...), bad...))
	us.curTime = 4000
	if err := us.parseMVD(); err == nil {
		t.Fatal("missing svc error")
	}
	if us.rb.Length() != len(good)+len(bad) || us.mvdTime.valid {
		t.Fatal("error advanced cache/queue")
	}
}

// Repeated partial appends and normalized discards must keep the cached prefix
// equal to a fresh full scan, even when records cross borrowed span boundaries.
func TestMVDTimeRepeatedAppendConsume(t *testing.T) {
	fixtures := []struct {
		wire      []byte
		elapsedMS int64
	}{
		{[]byte{13, 6, 2, 0, 0, 0, 32, 7}, 13},
		{[]byte{7, 4, 0, 0, 0, 0}, 7},
		{[]byte{9, 2, 0, 0, 0, 0, 0, 0, 0, 0}, 9},
		{[]byte{0, 3, 0, 0, 0, 0, 3, 0, 0, 0, 10, 0, 42}, 42},
		{[]byte{5, 3, 1, 0, 0, 0, 0, 0, 0, 0}, 5},
		{[]byte{11, 5, 0, 0, 0, 0}, 11},
	}
	var wire []byte
	for repeat := 0; repeat < 64; repeat++ {
		for _, fixture := range fixtures {
			wire = append(wire, fixture.wire...)
		}
	}
	for _, tc := range []struct {
		name    string
		elapsed bool
	}{{"container", false}, {"hidden elapsed", true}} {
		t.Run(tc.name, func(t *testing.T) {
			var cache mvdTimeCache
			var pending []byte
			pos, record := 0, 0
			check := func(step int) {
				t.Helper()
				complete, want := consistantMVD(pending, tc.elapsed)
				mid := len(pending) / 2
				view := byteView{parts: [][]byte{pending[:mid], pending[mid:]}, size: len(pending)}
				for query := 0; query < 2; query++ {
					if got := cache.pending(view, tc.elapsed); got != want || cache.bytes != complete {
						t.Fatalf("step %d query %d: time=%d want=%d prefix=%d want=%d", step, query, got, want, cache.bytes, complete)
					}
				}
			}
			chunks := []int{1, 5, 17, 2, 31, 7}
			for step := 0; pos < len(wire) || len(pending) > 0; step++ {
				if step > len(wire)*2 {
					t.Fatal("synthetic stream stopped making progress")
				}
				n := min(chunks[step%len(chunks)], len(wire)-pos)
				pending = append(pending, wire[pos:pos+n]...)
				pos += n
				check(step)
				used := 0
				var rawMS int64
				for count := 0; count < step%4; count++ {
					fixture := fixtures[record%len(fixtures)]
					if len(pending)-used < len(fixture.wire) {
						break
					}
					if tc.elapsed {
						rawMS += fixture.elapsedMS
					} else {
						rawMS += int64(fixture.wire[0])
					}
					pending[used] = 200 // Playback may normalize timestamps before discard.
					used += len(fixture.wire)
					record++
				}
				pending = pending[used:]
				cache.consume(used, rawMS, tc.elapsed)
				check(step)
			}
			if record != 64*len(fixtures) || cache.bytes != 0 || cache.milliseconds != 0 {
				t.Fatal("stream did not drain exactly", record, cache)
			}
		})
	}
}
