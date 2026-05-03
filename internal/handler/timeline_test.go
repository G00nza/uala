package handler_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"uala/internal/domain"
	"uala/internal/handler"

	"github.com/google/uuid"
)

func TestIntegration_Timeline_Get(t *testing.T) {
	if testDB == nil {
		t.Skip("integration only")
	}
	truncate(t)
	flushRedis(t)

	// Arrange
	aliceID := uuid.New()
	bobID := uuid.New()
	tweetID := uuid.New()
	seedUser(t, aliceID, "alice")
	seedUser(t, bobID, "bob")
	seedTweet(t, tweetID, aliceID, "hello from alice", time.Now())
	seedFollow(t, bobID, aliceID)
	srv := testServer(t)

	// Act
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/timeline", nil)
	req.Header.Set("X-User-ID", bobID.String())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get timeline: %v", err)
	}
	defer resp.Body.Close()

	// Assert: response
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	tweets, ok := result["tweets"].([]any)
	if !ok {
		t.Fatalf("expected tweets array in response")
	}
	if len(tweets) != 1 {
		t.Fatalf("want 1 tweet, got %d", len(tweets))
	}
	tweet := tweets[0].(map[string]any)
	if tweet["content"] != "hello from alice" {
		t.Errorf("want content 'hello from alice', got %v", tweet["content"])
	}
	if tweet["username"] != "alice" {
		t.Errorf("want username 'alice', got %v", tweet["username"])
	}
}

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

func TestTimelineHandler_GetTimeline_AfterCursorPassedToService(t *testing.T) {
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
