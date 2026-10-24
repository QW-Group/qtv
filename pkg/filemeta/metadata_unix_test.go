//go:build linux || openbsd || dragonfly || solaris || illumos || darwin || freebsd || netbsd

package filemeta

import (
	"os"
	"syscall"
	"testing"
)

type fileInfoWithSys struct {
	os.FileInfo
	systemInfo any
}

func (info fileInfoWithSys) Sys() any { return info.systemInfo }

func TestSnapshotUnavailable(t *testing.T) {
	info, err := os.Stat(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, systemInfo := range []any{nil, (*syscall.Stat_t)(nil), &syscall.Stat_t{}} {
		wrapped := fileInfoWithSys{FileInfo: info, systemInfo: systemInfo}
		_, changeTime := Snapshot("", wrapped)
		if !changeTime.IsZero() {
			t.Fatalf("unavailable change time must disable caching: %v", changeTime)
		}
	}
}
