package filemeta

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotUnavailable(t *testing.T) {
	dir := t.TempDir()
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, changeTime := Snapshot(filepath.Join(dir, "missing.mvd"), info)
	if !changeTime.IsZero() {
		t.Fatal("failed metadata lookup must disable caching")
	}
}
