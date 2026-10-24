// Package filemeta reads metadata used to detect file changes without reading contents.
package filemeta

import (
	"os"
	"time"
)

// Snapshot returns file information and its status-change time (Unix ctime or
// Windows ChangeTime). It uses info where possible; on Windows it refreshes the
// information from the same handle used to query ChangeTime.
// A zero time means that change time is unavailable. Callers can then fall back
// to file identity, size and modification time to detect changes.
// Metadata alone does not provide a byte-for-byte integrity check.
func Snapshot(path string, info os.FileInfo) (os.FileInfo, time.Time) {
	return snapshot(path, info)
}
