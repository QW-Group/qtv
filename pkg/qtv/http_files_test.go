package qtv

import (
	"errors"
	"io/fs"
	"net/http"
	"testing"
	"testing/fstest"
)

func TestHTTPHidesSensitiveExtensionsRegardlessOfCase(t *testing.T) {
	files := fileHidingFileSystem{http.FS(fstest.MapFS{
		"settings.CfG": {},
		"private.KEY":  {},
		"public.txt":   {},
	})}
	for _, name := range []string{"settings.CfG", "private.KEY"} {
		if file, err := files.Open("/" + name); !errors.Is(err, fs.ErrPermission) {
			if file != nil {
				file.Close()
			}
			t.Errorf("open %s: got %v, want permission denied", name, err)
		}
	}
	dir, err := files.Open("/")
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	entries, err := dir.Readdir(-1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "public.txt" {
		t.Fatalf("directory listing = %v, want only public.txt", entries)
	}
}
