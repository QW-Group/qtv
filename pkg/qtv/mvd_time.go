package qtv

// mvdTimeCache accounts for the complete, unmodified container-time prefix.
// It belongs to the source consumer, independently of its byte storage.
type mvdTimeCache struct {
	bytes        int   // Complete prefix already inspected.
	milliseconds int64 // Original elapsed time of that prefix, including on arm32.
	elapsed      bool  // Hidden paused-time interpretation used for inspection.
	valid        bool  // Prefix still corresponds to the source's queued bytes.
}

// reset invalidates accounting after reset, interpretation change or failed IO.
func (c *mvdTimeCache) reset() {
	*c = mvdTimeCache{}
}

// pending inspects only newly completed records and never counts a partial tail.
func (c *mvdTimeCache) pending(v byteView, elapsed bool) int64 {
	if !c.valid || c.elapsed != elapsed || c.bytes > v.size {
		*c = mvdTimeCache{valid: true, elapsed: elapsed}
	}
	cur := v.cursor(c.bytes)
	for cur.left > 0 {
		header, state := inspectMVDRecord(&cur, elapsed)
		if state != mvdRecordComplete {
			break
		}
		c.bytes += header.size
		c.milliseconds += int64(header.playbackMS)
		cur.skip(header.size)
	}
	return c.milliseconds
}

// consume commits original time only after the matching byte discard succeeds.
func (c *mvdTimeCache) consume(n int, rawMS int64, elapsed bool) {
	if !c.valid || c.elapsed != elapsed || n < 0 || n > c.bytes || rawMS < 0 || rawMS > c.milliseconds {
		c.reset()
		return
	}
	c.bytes -= n
	c.milliseconds -= rawMS
}
