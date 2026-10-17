package qtv

import (
	"io"
	"os"
)

// MVDSV can flush a recording before it finishes; only its final disconnect
// marker distinguishes a completed demo from a paused or buffered recording.
func demoIsComplete(file io.ReadSeeker) bool {
	const end = "\x02EndOfDemo\x00"
	if _, err := file.Seek(-int64(len(end)), io.SeekEnd); err != nil {
		return false
	}
	var tail [len(end)]byte
	_, readErr := io.ReadFull(file, tail[:])
	_, seekErr := file.Seek(0, io.SeekStart)
	return readErr == nil && seekErr == nil && string(tail[:]) == end
}

func demoPathIsComplete(name string) bool {
	file, err := os.Open(name)
	if err != nil {
		return false
	}
	defer file.Close()
	info, err := file.Stat()
	return err == nil && info.Mode().IsRegular() && demoIsComplete(file)
}
