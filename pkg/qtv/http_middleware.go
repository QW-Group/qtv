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

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	w.Header().Del("Content-Length")
	return w.writer.Write(b)
}

func (w gzipResponseWriter) Close() error {
	return w.writer.Close()
}

func (w gzipResponseWriter) Flush() {
	if fl, ok := w.ResponseWriter.(http.Flusher); ok {
		// flush gzip writer first to ensure data is forwarded.
		_ = w.writer.Flush()
		fl.Flush()
	}
}

// compresses response when the client accepts gzip
func gzipHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// accepts gzip encoding?
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}

		// already encoded?
		if encoding := w.Header().Get("Content-Encoding"); encoding != "" {
			next.ServeHTTP(w, r)
			return
		}

		// wrap the response writer with gzip writer
		gzip_writer := gzip.NewWriter(w)
		gzip_res_writer := gzipResponseWriter{ResponseWriter: w, writer: gzip_writer}

		// set headers
		headers := w.Header()
		headers.Set("Content-Encoding", "gzip")
		headers.Add("Vary", "Accept-Encoding")
		headers.Del("Content-Length") // no longer valid once compressed.

		// serve the request
		defer gzip_res_writer.Close()
		next.ServeHTTP(gzip_res_writer, r)
	})
}
