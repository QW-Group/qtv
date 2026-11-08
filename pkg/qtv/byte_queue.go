package qtv

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
)

var (
	errByteQueueFull = errors.New("byte queue is full")
	errByteQueueBusy = errors.New("byte queue IO already active")
)

// queueBlock separates committed bytes from unused tail space.
type queueBlock struct {
	data []byte // Fixed-size backing storage.
	used int    // Committed prefix, only extended under the queue lock.
}

// byteQueue holds a bounded byte stream without mirroring its backing storage.
// Producers copy their input; a single consumer may borrow committed spans.
type byteQueue struct {
	mu            sync.Mutex    // Protects queue state, never held over IO.
	blocks        []queueBlock  // Committed blocks in stream order.
	head          int           // Consumed prefix of the first block.
	length        int           // Queued bytes, including a head borrowed by WriteTo.
	limit         int           // Logical budget captured at construction.
	blockSize     int           // Allocation unit, clipped to the logical limit.
	spares        [][]byte      // At most two drained blocks retained for reuse.
	viewParts     [][]byte      // Snapshot descriptors, owned by the single consumer.
	reading       bool          // ReadFrom is the exclusive producer while active.
	writing       bool          // WriteTo is the exclusive consumer while active.
	dataReady     chan struct{} // Coalesced notification of committed input.
	spaceReady    chan struct{} // Coalesced notification of consumer progress.
	writeProgress chan struct{} // Coalesced notification of positive IO writes.
}

// newByteQueue allocates metadata only; payload blocks are allocated on demand.
func newByteQueue(limit, blockSize int) (*byteQueue, error) {
	if limit <= 0 || blockSize <= 0 {
		return nil, errors.New("byte queue sizes must be positive")
	}
	return &byteQueue{
		limit: limit, blockSize: min(limit, blockSize),
		dataReady: make(chan struct{}, 1), spaceReady: make(chan struct{}, 1),
		writeProgress: make(chan struct{}, 1),
	}, nil
}

// notifyQueue coalesces wakeups without pacing IO at the consumer's tick rate.
func notifyQueue(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Length includes bytes currently borrowed by an IO writer.
func (q *byteQueue) Length() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.length
}

// Capacity is the immutable logical byte budget, not allocated memory.
func (q *byteQueue) Capacity() int { return q.limit }

// Available reports the current unused logical budget.
func (q *byteQueue) Available() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.limit - q.length
}

// IsEmpty reports whether all committed bytes have been consumed.
func (q *byteQueue) IsEmpty() bool { return q.Length() == 0 }

// tailLocked returns writable space, reusing drained storage before allocating.
// The caller holds mu and commits only bytes past the existing tail prefix.
func (q *byteQueue) tailLocked() *queueBlock {
	if len(q.blocks) == 0 || q.blocks[len(q.blocks)-1].used == q.blockSize {
		var b []byte
		if len(q.spares) > 0 {
			last := len(q.spares) - 1
			b = q.spares[last]
			q.spares[last] = nil
			q.spares = q.spares[:last]
		}
		if b == nil {
			b = make([]byte, q.blockSize)
		}
		q.blocks = append(q.blocks, queueBlock{data: b})
	}
	return &q.blocks[len(q.blocks)-1]
}

// appendLocked copies bytes already checked against the budget.
func (q *byteQueue) appendLocked(p []byte) {
	q.length += len(p)
	for len(p) > 0 {
		tail := q.tailLocked()
		n := copy(tail.data[tail.used:], p)
		tail.used += n
		p = p[n:]
	}
	notifyQueue(q.dataReady)
}

// WriteFull appends an owned record atomically, or leaves the queue unchanged.
func (q *byteQueue) WriteFull(p []byte) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.reading {
		return 0, errByteQueueBusy
	}
	if len(p) > q.limit-q.length {
		return 0, errByteQueueFull
	}
	q.appendLocked(p)
	return len(p), nil
}

// Write implements io.Writer with an explicit error on a partial append.
func (q *byteQueue) Write(p []byte) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.reading {
		return 0, errByteQueueBusy
	}
	n := min(len(p), q.limit-q.length)
	q.appendLocked(p[:n])
	if n != len(p) {
		return n, errByteQueueFull
	}
	return n, nil
}

// WriteString copies only accepted bytes directly into owned blocks, so a
// partially flushed reply never converts its whole pending suffix to []byte.
func (q *byteQueue) WriteString(s string) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.reading {
		return 0, errByteQueueBusy
	}
	n := min(len(s), q.limit-q.length)
	pending := s[:n]
	q.length += n
	for len(pending) > 0 {
		tail := q.tailLocked()
		copied := copy(tail.data[tail.used:], pending)
		tail.used += copied
		pending = pending[copied:]
	}
	notifyQueue(q.dataReady)
	if n != len(s) {
		return n, errByteQueueFull
	}
	return n, nil
}

// discardLocked releases consumed blocks, retaining at most two for reuse.
func (q *byteQueue) discardLocked(n int) {
	q.length -= n
	removed := 0
	for n > 0 {
		b := &q.blocks[removed]
		avail := b.used - q.head
		if n < avail {
			q.head += n
			break
		}
		n -= avail
		q.head = 0
		if len(q.spares) < 2 {
			q.spares = append(q.spares, b.data)
		}
		removed++
	}
	if removed > 0 {
		remaining := copy(q.blocks, q.blocks[removed:])
		clear(q.blocks[remaining:])
		q.blocks = q.blocks[:remaining]
	}
	notifyQueue(q.spaceReady)
}

// Discard consumes a validated prefix after the borrowing consumer finishes.
func (q *byteQueue) Discard(n int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.writing {
		return errByteQueueBusy
	}
	if n < 0 || n > q.length {
		return errors.New("byte queue discard exceeds queued data")
	}
	q.discardLocked(n)
	return nil
}

// appendSpansLocked borrows n committed bytes into consumer-owned descriptors.
// The caller holds mu; payload blocks remain live until that consumer discards them.
func (q *byteQueue) appendSpansLocked(parts [][]byte, n int) [][]byte {
	for i, b := range q.blocks {
		if n == 0 {
			break
		}
		start := 0
		if i == 0 {
			start = q.head
		}
		size := min(n, b.used-start)
		parts = append(parts, b.data[start:start+size])
		n -= size
	}
	return parts
}

// snapshot borrows stable committed spans until the consumer discards or resets.
// There must be no other snapshot/IO consumer until this view is released.
func (q *byteQueue) snapshot() byteView {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.viewParts = q.appendSpansLocked(q.viewParts[:0], q.length)
	return byteView{parts: q.viewParts, size: q.length}
}

// Reset is valid only after IO workers stop and borrowed views are released.
func (q *byteQueue) Reset() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.reading || q.writing {
		panic("byte queue reset during active IO")
	}
	q.blocks, q.spares, q.viewParts = nil, nil, nil
	q.head, q.length = 0, 0
	for _, ch := range []chan struct{}{q.dataReady, q.spaceReady, q.writeProgress} {
		select {
		case <-ch:
		default:
		}
	}
}

// ReadFromWithContext copies outside-lock reads into the available byte budget.
// The owner must close the reader to interrupt a blocked Read on cancellation.
func (q *byteQueue) ReadFromWithContext(ctx context.Context, r io.Reader) (int64, error) {
	return q.readFromWithContext(ctx, r, nil)
}

// readFromWithContext optionally groups short reads on consumer ticks. Full
// reads continue immediately, so a permit never caps input at one block/tick.
// A short read is only a scheduling hint; its bytes and errors retain normal IO semantics.
func (q *byteQueue) readFromWithContext(ctx context.Context, r io.Reader, readPermit <-chan struct{}) (total int64, err error) {
	q.mu.Lock()
	if q.reading {
		q.mu.Unlock()
		return 0, errByteQueueBusy
	}
	q.reading = true
	q.mu.Unlock()
	defer func() { q.mu.Lock(); q.reading = false; q.mu.Unlock() }()
	scratch := make([]byte, q.blockSize)
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		avail := min(q.Available(), len(scratch))
		if avail == 0 {
			select {
			case <-ctx.Done():
				return total, ctx.Err()
			case <-q.spaceReady:
			}
			continue
		}
		n, err := r.Read(scratch[:avail])
		if n > 0 {
			q.mu.Lock()
			q.appendLocked(scratch[:n])
			q.mu.Unlock()
			total += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return total, err
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
		if readPermit != nil && n < avail {
			select {
			case <-ctx.Done():
				return total, ctx.Err()
			case <-readPermit:
			}
		}
	}
}

// WriteToWithContext keeps the borrowed head queued until Write returns.
// The owner must close the writer to interrupt a blocked Write on cancellation.
func (q *byteQueue) WriteToWithContext(ctx context.Context, w io.Writer) (int64, error) {
	return q.writeToWithContext(ctx, w, nil)
}

// Limit one gathered TCP operation so slow viewers still return progress/error
// without borrowing an entire large initial-state queue in one WriteTo call.
const queueTCPWriteLimit = 64 * 1024

// writeToWithContext optionally waits for a consumer tick between entire queued
// prefixes. A batch spans any number of blocks; positive writes still report
// progress immediately, independently of the next-batch permit.
func (q *byteQueue) writeToWithContext(ctx context.Context, w io.Writer, batchPermit <-chan struct{}) (total int64, err error) {
	q.mu.Lock()
	if q.writing {
		q.mu.Unlock()
		return 0, errByteQueueBusy
	}
	q.writing = true
	q.mu.Unlock()
	defer func() { q.mu.Lock(); q.writing = false; q.mu.Unlock() }()
	_, gather := w.(*net.TCPConn)
	var spans net.Buffers
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		q.mu.Lock()
		if q.length == 0 {
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				return total, ctx.Err()
			case <-q.dataReady:
			}
			continue
		}
		remaining := q.length
		q.mu.Unlock()
		for remaining > 0 {
			if err := ctx.Err(); err != nil {
				return total, err
			}
			q.mu.Lock()
			b := q.blocks[0]
			size := remaining
			if gather {
				size = min(size, queueTCPWriteLimit)
			}
			p := b.data[q.head : q.head+min(b.used-q.head, size)]
			if gather && len(p) < size {
				spans = q.appendSpansLocked(spans[:0], size)
			}
			q.mu.Unlock()
			var n int
			var err error
			if len(spans) > 0 {
				// Buffers.WriteTo mutates its descriptors, not the borrowed payload.
				buffers := spans
				written, writeErr := buffers.WriteTo(w)
				n, err = int(written), writeErr
				clear(spans)
				spans = spans[:0]
			} else {
				n, err = w.Write(p)
			}
			if n > 0 {
				q.mu.Lock()
				q.discardLocked(n)
				q.mu.Unlock()
				total += int64(n)
				remaining -= n
				notifyQueue(q.writeProgress)
			}
			if err != nil {
				return total, err
			}
			if n == 0 {
				return total, io.ErrNoProgress
			}
		}
		if batchPermit != nil {
			select {
			case <-ctx.Done():
				return total, ctx.Err()
			case <-batchPermit:
			}
		}
	}
}
