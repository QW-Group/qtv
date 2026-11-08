package qtv

import "bytes"

// byteView borrows committed spans until the owning consumer discards them.
type byteView struct {
	parts [][]byte // Descriptor storage is owned by the single consumer.
	size  int      // Total committed bytes represented by parts.
}

// byteViewOf adapts contiguous input without copying its payload.
func byteViewOf(b []byte) byteView {
	return byteView{parts: [][]byte{b}, size: len(b)}
}

// cursor starts at a validated byte offset, independently of other cursors.
func (v byteView) cursor(offset int) byteCursor {
	c := byteCursor{parts: v.parts, left: v.size}
	c.skip(offset)
	return c
}

// release forgets borrowed spans without retaining the payload after a pass.
func (v byteView) release() {
	clear(v.parts)
}

// byteCursor reads across spans; only a straddling peek copies into scratch.
type byteCursor struct {
	parts [][]byte // Borrowed spans, valid for the lifetime of their view.
	index int      // Current span.
	off   int      // Consumed prefix of the current span.
	left  int      // Remaining committed bytes.
}

// peek leaves the cursor unchanged; scratch must fit a straddling result.
func (c *byteCursor) peek(n int, scratch []byte) []byte {
	if n < 0 || n > c.left {
		return nil
	}
	if n == 0 {
		return scratch[:0]
	}
	index, off := c.index, c.off
	for index < len(c.parts) && off == len(c.parts[index]) {
		index++
		off = 0
	}
	b := c.parts[index][off:]
	if len(b) >= n {
		return b[:n]
	}
	out := scratch[:n]
	at := copy(out, b)
	for index++; at < n; index++ {
		at += copy(out[at:], c.parts[index])
	}
	return out
}

// skip advances over a prefix known to fit in the remaining view.
func (c *byteCursor) skip(n int) {
	c.left -= n
	for n > 0 {
		avail := len(c.parts[c.index]) - c.off
		if n < avail {
			c.off += n
			return
		}
		n -= avail
		c.index++
		c.off = 0
	}
}

// read borrows contiguous bytes, using reusable scratch only across blocks.
func (c *byteCursor) read(n int, scratch *[]byte) []byte {
	probe := *c
	for probe.index < len(probe.parts) && probe.off == len(probe.parts[probe.index]) {
		probe.index++
		probe.off = 0
	}
	if n > 0 && n <= probe.left && len(probe.parts[probe.index])-probe.off >= n {
		return probe.parts[probe.index][probe.off : probe.off+n]
	}
	if cap(*scratch) < n {
		*scratch = make([]byte, n)
	}
	return c.peek(n, (*scratch)[:n])
}

// lineSize finds a complete newline-terminated line without copying its bytes.
func (c *byteCursor) lineSize() int {
	n := 0
	for i := c.index; i < len(c.parts); i++ {
		b := c.parts[i]
		if i == c.index {
			b = b[c.off:]
		}
		if at := bytes.IndexByte(b, '\n'); at >= 0 {
			return n + at + 1
		}
		n += len(b)
	}
	return 0
}
