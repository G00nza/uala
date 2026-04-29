package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"uala/internal/metrics"
)

func TestMiddleware_PassesThroughResponse(t *testing.T) {
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"id":"abc"}`))
	}))

	req := httptest.NewRequest(http.MethodPost, "/tweets", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d", rr.Code)
	}
	if rr.Body.String() != `{"id":"abc"}` {
		t.Fatalf("want body '{\"id\":\"abc\"}', got %s", rr.Body.String())
	}
}

func TestMiddleware_DefaultsTo200WhenWriteHeaderNotCalled(t *testing.T) {
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
}

func TestMiddleware_CapturesNonOKStatus(t *testing.T) {
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rr.Code)
	}
}
