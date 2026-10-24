package qtv

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildDemoListRehashesReplacementWithSameMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "match.mvd")
	if err := os.WriteFile(path, []byte("AAAA"), 0600); err != nil {
		t.Fatal(err)
	}

	first, err := buildDemoList(dir, nil)
	if err != nil || len(first) != 1 {
		t.Fatalf("initial scan: %d entries, err=%v", len(first), err)
	}

	replacement := filepath.Join(dir, "replacement.tmp")
	if err := os.WriteFile(replacement, []byte("BBBB"), 0600); err != nil {
		t.Fatal(err)
	}
	mtime := first[0].FileInfo.ModTime()
	if err := os.Chtimes(replacement, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != first[0].FileInfo.Size() || !info.ModTime().Equal(mtime) {
		t.Fatal("replacement did not preserve cache-key metadata")
	}

	list, err := buildDemoList(dir, first)
	if err != nil || len(list) != 1 {
		t.Fatalf("second scan: %d entries, err=%v", len(list), err)
	}
	want, err := XXH3Sum(path)
	if err != nil {
		t.Fatal(err)
	}
	if list[0].Hash.XXH3 != want {
		t.Fatalf("replacement reused stale hash %s; want %s", list[0].Hash.XXH3, want)
	}
}

func TestBuildDemoListRehashesInPlaceRewriteWithSameMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "match.mvd")
	if err := os.WriteFile(path, []byte("AAAA"), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := buildDemoList(dir, nil)
	if err != nil || len(first) != 1 {
		t.Fatalf("initial scan: %d entries, err=%v", len(first), err)
	}

	if first[0].changeTime.IsZero() {
		t.Skip("filesystem does not expose change time; in-place rewrites with preserved metadata cannot be detected")
	}

	if err := os.WriteFile(path, []byte("BBBB"), 0600); err != nil {
		t.Fatal(err)
	}
	mtime := first[0].FileInfo.ModTime()
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != first[0].FileInfo.Size() || !info.ModTime().Equal(mtime) || !os.SameFile(info, first[0].FileInfo) {
		t.Fatal("rewrite did not preserve file identity and cache-key metadata")
	}

	list, err := buildDemoList(dir, first)
	if err != nil || len(list) != 1 {
		t.Fatalf("second scan: %d entries, err=%v", len(list), err)
	}
	want, err := XXH3Sum(path)
	if err != nil {
		t.Fatal(err)
	}
	if list[0].Hash.XXH3 != want {
		t.Fatalf("in-place rewrite reused stale hash %s; want %s", list[0].Hash.XXH3, want)
	}
}

func TestBuildDemoListReusesHashWithUnchangedMetadata(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "match.mvd"), []byte("AAAA"), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := buildDemoList(dir, nil)
	if err != nil || len(first) != 1 {
		t.Fatalf("initial scan: %d entries, err=%v", len(first), err)
	}
	// A distinct cached value makes an unnecessary content read observable.
	first[0].Hash.XXH3 = "cached-hash"
	list, err := buildDemoList(dir, first)
	if err != nil || len(list) != 1 {
		t.Fatalf("second scan: %d entries, err=%v", len(list), err)
	}
	if list[0].Hash.XXH3 != "cached-hash" {
		t.Fatal("unchanged file was rehashed")
	}
}

func TestBuildDemoListRehashesWhenChangeTimeAvailabilityChanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "match.mvd")
	if err := os.WriteFile(path, []byte("AAAA"), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := buildDemoList(dir, nil)
	if err != nil || len(first) != 1 {
		t.Fatalf("initial scan: %d entries, err=%v", len(first), err)
	}
	if first[0].changeTime.IsZero() {
		first[0].changeTime = time.Unix(1, 0)
	} else {
		first[0].changeTime = time.Time{}
	}
	first[0].Hash.XXH3 = "stale-hash"
	list, err := buildDemoList(dir, first)
	if err != nil || len(list) != 1 {
		t.Fatalf("second scan: %d entries, err=%v", len(list), err)
	}
	want, err := XXH3Sum(path)
	if err != nil {
		t.Fatal(err)
	}
	if list[0].Hash.XXH3 != want {
		t.Fatal("hash was reused after change-time availability changed")
	}
}
