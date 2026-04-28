package handler_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/handler"
)

func TestTweetHandler_Create_OK(t *testing.T) {
	userID := uuid.New()
	tweetID := uuid.New()
	svc := &mockTweetSvc{tweet: &domain.Tweet{ID: tweetID, UserID: userID, Content: "hello", CreatedAt: time.Now()}}
	h := handler.NewTweetHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/tweets",
		bytes.NewBufferString(`{"content":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", userID.String())
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d", rec.Code)
	}
	var resp map[string]string
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp["id"] != tweetID.String() {
		t.Fatalf("want %s, got %s", tweetID, resp["id"])
	}
}

func TestTweetHandler_Create_MissingUserID(t *testing.T) {
	h := handler.NewTweetHandler(&mockTweetSvc{})

	req := httptest.NewRequest(http.MethodPost, "/tweets",
		bytes.NewBufferString(`{"content":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTweetHandler_Create_EmptyContent(t *testing.T) {
	h := handler.NewTweetHandler(&mockTweetSvc{})

	req := httptest.NewRequest(http.MethodPost, "/tweets",
		bytes.NewBufferString(`{"content":""}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTweetHandler_Create_ContentTooLong(t *testing.T) {
	h := handler.NewTweetHandler(&mockTweetSvc{})

	body := `{"content":"` + strings.Repeat("a", 281) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/tweets", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestTweetHandler_Create_UserNotFound(t *testing.T) {
	svc := &mockTweetSvc{err: domain.ErrNotFound}
	h := handler.NewTweetHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/tweets",
		bytes.NewBufferString(`{"content":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rec.Code)
	}
}
