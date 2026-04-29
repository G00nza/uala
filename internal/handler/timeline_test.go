package handler_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/handler"
)

func TestTimelineHandler_GetTimeline_Empty(t *testing.T) {
	svc := &mockTimelineSvc{items: []domain.TweetItem{}}
	h := handler.NewTimelineHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/timeline", nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	var resp map[string]any
	json.NewDecoder(rec.Body).Decode(&resp)
	tweets, ok := resp["tweets"].([]any)
	if !ok || len(tweets) != 0 {
		t.Fatalf("want empty tweets array, got %v", resp)
	}
}

func TestTimelineHandler_GetTimeline_MissingUserID(t *testing.T) {
	h := handler.NewTimelineHandler(&mockTimelineSvc{})

	req := httptest.NewRequest(http.MethodGet, "/timeline", nil)
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTimelineHandler_GetTimeline_UserNotFound(t *testing.T) {
	svc := &mockTimelineSvc{err: domain.ErrNotFound}
	h := handler.NewTimelineHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/timeline", nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rec.Code)
	}
}
