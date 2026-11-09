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

func TestByteCursorReadBorrowAndScratch(t *testing.T) {
	first, last := []byte("abcd"), []byte("ef")
	v := byteView{parts: [][]byte{nil, first, nil, last, nil}, size: 6}
	var scratch []byte
	c := v.cursor(1)
	got := c.read(2, &scratch)
	if string(got) != "bc" || &got[0] != &first[1] || scratch != nil {
		t.Fatal("contiguous read did not borrow payload")
	}
	if c.left != 5 || string(c.peek(2, nil)) != "bc" {
		t.Fatal("read advanced cursor")
	}
	c = v.cursor(2)
	scratch = make([]byte, 0, 8)
	backing := scratch[:cap(scratch)]
	for _, tc := range []struct {
		n    int
		want string
	}{{4, "cdef"}, {3, "cde"}} {
		got = c.read(tc.n, &scratch)
		if string(got) != tc.want || &got[0] != &backing[0] {
			t.Fatal("cross-span read did not reuse scratch", string(got))
		}
		got[0] = 'X'
		if string(first) != "abcd" || string(last) != "ef" {
			t.Fatal("cross-span read changed source payload")
		}
	}
	scratch = make([]byte, 1)
	small := scratch
	got = c.read(4, &scratch)
	if string(got) != "cdef" || len(scratch) != 4 || &got[0] == &small[0] {
		t.Fatal("scratch did not grow for cross-span read")
	}
	for _, empty := range []byteView{{}, {parts: [][]byte{nil, {}}, size: 0}, v} {
		c = empty.cursor(empty.size)
		scratch = nil
		if got := c.read(0, &scratch); len(got) != 0 || scratch != nil || c.left != 0 {
			t.Fatal("zero read at end changed cursor or allocated scratch")
		}
	}
	c = v.cursor(0)
	scratch = nil
	if got := c.read(2, &scratch); string(got) != "ab" || &got[0] != &first[0] {
		t.Fatal("read did not skip leading empty span")
	}
}

func TestByteCursorLineSize(t *testing.T) {
	for _, tc := range []struct {
		name         string
		parts        [][]byte
		offset, want int
	}{
		{"empty", nil, 0, 0},
		{"empty spans", [][]byte{nil, {}}, 0, 0},
		{"incomplete", [][]byte{[]byte("ab"), nil, []byte("c")}, 1, 0},
		{"leading newline", [][]byte{nil, []byte("\n"), nil, []byte("x")}, 0, 1},
		{"newline at next span", [][]byte{[]byte("abc"), nil, []byte("\n"), []byte("tail")}, 0, 4},
		{"middle of split line", [][]byte{[]byte("ab"), nil, []byte("c\n"), []byte("tail\n")}, 1, 3},
		{"next line", [][]byte{[]byte("ab"), nil, []byte("c\n"), []byte("tail\n")}, 4, 5},
		{"end", [][]byte{[]byte("abc\n"), nil}, 4, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			size := 0
			for _, p := range tc.parts {
				size += len(p)
			}
			v := byteView{parts: tc.parts, size: size}
			c := v.cursor(tc.offset)
			for query := 0; query < 2; query++ {
				if got := c.lineSize(); got != tc.want || c.left != size-tc.offset {
					t.Fatal("line lookup consumed bytes or found wrong boundary", got, tc.want, c.left)
				}
			}
		})
	}
}
