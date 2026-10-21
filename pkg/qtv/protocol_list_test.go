package qtv

import (
	"bytes"
	"fmt"
	"testing"
)

// TestReadListFullCatalog checks that the wire terminator needs no spare array slot.
func TestReadListFullCatalog(t *testing.T) {
	for _, count := range []int{0, 1, 254, 255} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			// Literal wire layout: cursor, NUL-terminated names, empty name, continuation.
			wire := []byte{0}
			for i := 1; i <= count; i++ {
				wire = append(wire, []byte(fmt.Sprintf("s%d\x00", i))...)
			}
			wire = append(wire, 0, 0, 0xa5)
			qp := &qProtocol{r: newNetMsgR(wire, false)}
			list := make([]string, 256)
			next, err := qp.readList(list, false)
			if err != nil || next != 0 || qp.r.error {
				t.Fatalf("readList: next=%d, err=%v, badread=%v", next, err, qp.r.error)
			}
			if qp.r.GetByte() != 0xa5 {
				t.Fatal("did not consume exactly through the continuation byte")
			}
			for i := 1; i <= count; i++ {
				if list[i] != fmt.Sprintf("s%d", i) {
					t.Fatalf("slot %d: %q", i, list[i])
				}
			}
		})
	}
}

// TestReadListRejectsNameBeyondCapacity distinguishes a terminator from an extra name.
func TestReadListRejectsNameBeyondCapacity(t *testing.T) {
	qp := &qProtocol{r: newNetMsgR([]byte{0, 'a', 0, 'b', 0, 0, 0}, false)}
	if _, err := qp.readList(make([]string, 2), false); err == nil {
		t.Fatal("accepted a second name with only one usable slot")
	}
}

// TestReadListEmptyCursorBounds allows the last slot, but not a cursor beyond the catalog.
func TestReadListEmptyCursorBounds(t *testing.T) {
	for _, test := range []struct {
		name     string // Boundary scenario.
		capacity int    // Storage size, including the reserved zero slot.
		extended bool   // Whether the wire cursor occupies two bytes.
		wire     []byte // Literal cursor, empty name, zero continuation, and following marker.
		wantErr  bool   // Whether the starting cursor lies outside the catalog.
	}{
		{"last-small-slot", 2, false, []byte{1, 0, 0, 0xa5}, false},
		{"past-small-catalog", 2, false, []byte{2, 0, 0, 0xa5}, true},
		{"last-legacy-slot", 256, false, []byte{255, 0, 0, 0xa5}, false},
		{"last-extended-slot", 4096, true, []byte{255, 15, 0, 0, 0xa5}, false},
		{"past-extended-catalog", 4096, true, []byte{0, 16, 0, 0, 0xa5}, true},
		{"maximum-short-cursor", 4096, true, []byte{255, 255, 0, 0, 0xa5}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			qp := &qProtocol{r: newNetMsgR(test.wire, false)}
			list := make([]string, test.capacity)
			for i := range list {
				list[i] = "existing"
			}
			next, err := qp.readList(list, test.extended)
			if (err != nil) != test.wantErr {
				t.Fatalf("readList: next=%d err=%v, wantErr=%v", next, err, test.wantErr)
			}
			if !test.wantErr {
				if next != 0 || qp.r.error || qp.r.GetByte() != 0xa5 || qp.r.RPos() != len(test.wire) {
					t.Fatal("full-catalog terminator did not consume exactly through the continuation byte")
				}
			}
			for i, name := range list {
				if name != "existing" {
					t.Fatalf("empty chunk changed slot %d to %q", i, name)
				}
			}
		})
	}
}

// TestReadListClearsTerminatorSlot preserves replacement of an earlier list tail.
func TestReadListClearsTerminatorSlot(t *testing.T) {
	qp := &qProtocol{r: newNetMsgR([]byte{0, 'a', 0, 0, 0}, false)}
	list := []string{"", "old", "stale"}
	if _, err := qp.readList(list, false); err != nil || list[1] != "a" || list[2] != "" {
		t.Fatalf("readList: %v, list=%q", err, list)
	}
}

// TestPutListEndsFullCatalog checks the final wire bytes without relying on our reader.
func TestPutListEndsFullCatalog(t *testing.T) {
	qp := &qProtocol{}
	msg := newNetMsgW(make([]byte, 1024), false)
	// A list with one usable slot, occupied by "a", has no spare sentinel slot.
	next := qp.putList(msg, 0, []string{"", "a"}, svc_soundlist, svc_fte_soundlistshort_UNUSED)
	want := []byte{46, 0, 'a', 0, 0, 0}
	if next != -1 || msg.error || !bytes.Equal(msg.Bytes(), want) {
		t.Fatalf("next=%d, badwrite=%v, bytes=%v, want %v", next, msg.error, msg.Bytes(), want)
	}
}

// TestListCatalogRoundTrip exercises chunk continuation and completion at the old limit.
func TestListCatalogRoundTrip(t *testing.T) {
	for _, count := range []int{0, 1, 254, 255} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			list := make([]string, 256)
			for i := 1; i <= count; i++ {
				list[i] = fmt.Sprintf("sound/slot%03d.wav", i)
			}
			decoded := make([]string, len(list))
			qp := &qProtocol{}
			cursor := 0
			for chunks := 0; ; chunks++ {
				if chunks >= 20 {
					t.Fatal("catalog writer did not terminate")
				}
				msg := newNetMsgW(make([]byte, 2048), false)
				next := qp.putList(msg, cursor, list, svc_soundlist, svc_fte_soundlistshort_UNUSED)
				if msg.error || msg.Bytes()[0] != 46 {
					t.Fatalf("invalid legacy chunk: %v", msg.Bytes())
				}
				qp.r = newNetMsgR(msg.Bytes()[1:], false)
				continuation, err := qp.readList(decoded, false)
				if err != nil || qp.r.error || qp.r.RPos() != len(msg.Bytes())-1 {
					t.Fatalf("chunk parse: %v, badread=%v", err, qp.r.error)
				}
				if next < 0 {
					if continuation != 0 {
						t.Fatalf("final continuation=%d", continuation)
					}
					break
				}
				if next <= cursor || int(continuation) != next {
					t.Fatalf("invalid continuation: cursor=%d next=%d wire=%d", cursor, next, continuation)
				}
				cursor = next
			}
			for i, name := range list {
				if decoded[i] != name {
					t.Fatalf("slot %d: got %q want %q", i, decoded[i], name)
				}
			}
		})
	}
}
