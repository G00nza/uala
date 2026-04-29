package metrics

import (
	"net/http"
	"strconv"
	"time"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Middleware records http_request_duration_seconds and http_requests_total per request.
// If WriteHeader is never called (response body written directly), status defaults to 200.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r)
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(sw.status)
		HTTPRequestDuration.WithLabelValues(r.URL.Path, r.Method, status).Observe(duration)
		HTTPRequestsTotal.WithLabelValues(r.URL.Path, r.Method, status).Inc()
	})
}
