package qtv

import (
	"bytes"
	"testing"
)

func TestByteCursorPeekAndSkip(t *testing.T) {
	for _, tc := range []struct {
		name  string
		parts [][]byte
	}{
		{"contiguous", [][]byte{[]byte("abcdef")}},
		{"split and empty", [][]byte{nil, []byte("ab"), {}, []byte("c"), nil, []byte("def"), nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := byteView{parts: tc.parts, size: 6}
			for offset := 0; offset <= 6; offset++ {
				c := v.cursor(offset)
				scratch := make([]byte, 6)
				for n := 0; n <= 6-offset; n++ {
					for query := 0; query < 2; query++ {
						if got := c.peek(n, scratch); string(got) != "abcdef"[offset:offset+n] || c.left != 6-offset {
							t.Fatalf("offset=%d n=%d query=%d: got=%q left=%d", offset, n, query, got, c.left)
						}
					}
				}
				for _, n := range []int{-1, 7 - offset} {
					if got := c.peek(n, scratch); got != nil {
						t.Fatalf("out-of-range peek n=%d: %q", n, got)
					}
				}
				if got := c.peek(c.left, scratch); string(got) != "abcdef"[offset:] {
					t.Fatal("invalid peek changed cursor", offset, string(got))
				}
			}
			c := v.cursor(0)
			other := v.cursor(1)
			for _, step := range []struct {
				skip int
				want string
			}{{0, "abcdef"}, {1, "bcdef"}, {1, "cdef"}, {1, "def"}, {3, ""}} {
				c.skip(step.skip)
				if got := c.peek(c.left, make([]byte, 6)); string(got) != step.want {
					t.Fatal("skip across spans", step.skip, string(got), step.want)
				}
			}
			if got := other.peek(3, make([]byte, 3)); string(got) != "bcd" {
				t.Fatal("cursors share position", string(got))
			}
		})
	}
}

func TestByteViewBorrowAndRelease(t *testing.T) {
	data := []byte("abcd")
	view := byteViewOf(data)
	cursor := view.cursor(1)
	got := cursor.peek(2, nil)
	if string(got) != "bc" || &got[0] != &data[1] {
		t.Fatal("contiguous view copied payload")
	}
	storage := [][]byte{data[:2], nil, data[2:]}
	split := byteView{parts: storage, size: 4}
	split.release()
	for i, p := range storage {
		if p != nil {
			t.Fatal("release retained payload reference", i)
		}
	}
	view.release()
	if view.parts[0] != nil || !bytes.Equal(data, []byte("abcd")) || string(got) != "bc" {
		t.Fatal("release changed payload or retained reference")
	}
}
