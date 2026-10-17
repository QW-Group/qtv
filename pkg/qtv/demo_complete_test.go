package qtv

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIncompleteDemosAreNotPublished(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.Mkdir("demos", 0755); err != nil {
		t.Fatal(err)
	}
	complete := strings.Repeat("x", 1024) + "\x02EndOfDemo\x00"
	files := map[string]string{
		"empty.mvd": "", "partial.mvd": strings.Repeat("x", 1024),
		"truncated.mvd":       complete[:len(complete)-1],
		"embedded-marker.mvd": complete + "unfinished",
		"complete.mvd":        complete, "readme.txt": "metadata",
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join("demos", name), []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	handler := http.FileServer(fileHidingFileSystem{http.Dir("demos")})
	for name := range files {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRecorder()
			handler.ServeHTTP(r, httptest.NewRequest("GET", "/"+name, nil))
			want := http.StatusNotFound
			if name == "complete.mvd" || name == "readme.txt" {
				want = http.StatusOK
			}
			if r.Code != want {
				t.Fatalf("status=%d, want %d", r.Code, want)
			}
			if want == http.StatusOK && r.Body.String() != files[name] {
				t.Fatal("download bytes changed")
			}
		})
	}
	r := httptest.NewRecorder()
	handler.ServeHTTP(r, httptest.NewRequest("GET", "/", nil))
	for _, name := range []string{"empty.mvd", "partial.mvd", "truncated.mvd", "embedded-marker.mvd"} {
		if strings.Contains(r.Body.String(), name) {
			t.Errorf("directory exposes %s", name)
		}
	}
	qtv, err := NewQTV(context.Background(), nil, []string{"qtv"})
	if err != nil {
		t.Fatal(err)
	}
	defer qtv.Stop()
	if err := qtv.updateDemoList(); err != nil {
		t.Fatal(err)
	}
	list := qtv.getDemoList()
	if len(list) != 1 || list[0].FileInfo.Name() != "complete.mvd" {
		t.Errorf("demo list = %+v", list)
	}
	stream, err := newUStreamFile(qtv, "file:partial.mvd")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Open(context.Background()); err == nil {
		stream.Close()
		t.Error("QTV opened incomplete demo")
	}
	if err := os.WriteFile("demos/partial.mvd", []byte(complete), 0644); err != nil {
		t.Fatal(err)
	}
	if err := qtv.updateDemoList(); err != nil {
		t.Fatal(err)
	}
	if len(qtv.getDemoList()) != 2 {
		t.Error("finalized demo not published")
	}
	if err := stream.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	stream.Close()
	r = httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/partial.mvd", nil)
	req.Header.Set("Range", "bytes=0-7")
	handler.ServeHTTP(r, req)
	if r.Code != http.StatusPartialContent || r.Body.String() != complete[:8] {
		t.Fatal("finalized range download changed")
	}
}
