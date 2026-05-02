package handler_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

func TestTimelineHandler_GetTimeline_BothCursors_Returns400(t *testing.T) {
	h := handler.NewTimelineHandler(&mockTimelineSvc{})

	url := "/timeline?after=" + uuid.New().String() + "&before=" + uuid.New().String()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTimelineHandler_GetTimeline_InvalidAfterUUID_Returns400(t *testing.T) {
	h := handler.NewTimelineHandler(&mockTimelineSvc{})

	req := httptest.NewRequest(http.MethodGet, "/timeline?after=not-a-uuid", nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTimelineHandler_GetTimeline_CursorsInResponse(t *testing.T) {
	firstID := uuid.New()
	lastID := uuid.New()
	svc := &mockTimelineSvc{items: []domain.TweetItem{
		{ID: firstID, UserID: uuid.New(), Username: "a", Content: "first", CreatedAt: time.Now().Add(-1 * time.Second)},
		{ID: lastID, UserID: uuid.New(), Username: "b", Content: "last", CreatedAt: time.Now().Add(-2 * time.Second)},
	}}
	h := handler.NewTimelineHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/timeline", nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["prev_cursor"] != firstID.String() {
		t.Fatalf("want prev_cursor=%s, got %v", firstID, resp["prev_cursor"])
	}
	if resp["next_cursor"] != lastID.String() {
		t.Fatalf("want next_cursor=%s, got %v", lastID, resp["next_cursor"])
	}
}

func TestTimelineHandler_GetTimeline_EmptyResult_NullCursors(t *testing.T) {
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
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["next_cursor"] != nil {
		t.Fatalf("want next_cursor=null, got %v", resp["next_cursor"])
	}
	if resp["prev_cursor"] != nil {
		t.Fatalf("want prev_cursor=null, got %v", resp["prev_cursor"])
	}
}

func TestTimelineHandler_GetTimeline_AfterPassedToService(t *testing.T) {
	afterID := uuid.New()
	svc := &mockTimelineSvc{items: []domain.TweetItem{}}
	h := handler.NewTimelineHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/timeline?after="+afterID.String(), nil)
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.GetTimeline(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	if svc.capturedAfter == nil || *svc.capturedAfter != afterID {
		t.Fatalf("want after=%s passed to svc, got %v", afterID, svc.capturedAfter)
	}
}
