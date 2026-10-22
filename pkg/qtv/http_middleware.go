package qtv

import (
	"compress/gzip"
	"net/http"
	"strings"
)

type gzipResponseWriter struct {
	http.ResponseWriter
	writer *gzip.Writer
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	w.Header().Del("Content-Length") // no longer valid once compressed
	return w.writer.Write(b)
}

func (w *gzipResponseWriter) Close() error {
	return w.writer.Close()
}

func (w *gzipResponseWriter) Flush() {
	if fl, ok := w.ResponseWriter.(http.Flusher); ok {
		_ = w.writer.Flush() // flush gzip writer first to ensure data is forwarded.
		fl.Flush()
	}
}

// compresses response when the client accepts gzip
func GzipHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// skip if client does not accept gzip
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}

		// wrap ResponseWriter
		gzipWriter, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
		gzipRespWriter := &gzipResponseWriter{
			ResponseWriter: w,
			writer:         gzipWriter,
		}

		// set headers
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")
		w.Header().Del("Content-Length")

		defer gzipRespWriter.Close() // ensure gzip writer is closed
		next.ServeHTTP(gzipRespWriter, r)
	})
}
