package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"uala/internal/domain"
	"uala/internal/handler"

	"github.com/google/uuid"
)

func TestIntegration_Tweet_Create(t *testing.T) {
	if testDB == nil {
		t.Skip("integration only")
	}
	truncate(t)
	flushRedis(t)

	// Arrange
	aliceID := uuid.New()
	seedUser(t, aliceID, "alice")
	pub := &spyTweetPublisher{}
	srv := testServerWith(t, pub, &noopFollowPublisher{})

	// Act
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/tweets",
		strings.NewReader(`{"content":"hello world"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", aliceID.String())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create tweet: %v", err)
	}
	defer resp.Body.Close()

	// Assert: response
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("want 201, got %d", resp.StatusCode)
	}
	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	tweetID := result["id"]

	// Assert: DB state
	var count int
	err = testDB.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM tweets WHERE id = $1 AND user_id = $2 AND content = $3",
		tweetID, aliceID, "hello world").Scan(&count)
	if err != nil || count != 1 {
		t.Fatalf("tweet not found in DB: %v", err)
	}

	// Assert: event published
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 1 {
		t.Fatalf("want 1 TweetCreatedEvent, got %d", len(pub.events))
	}
	if pub.events[0].UserID != aliceID {
		t.Errorf("event UserID: want %s, got %s", aliceID, pub.events[0].UserID)
	}
	if pub.events[0].Content != "hello world" {
		t.Errorf("event Content: want 'hello world', got %q", pub.events[0].Content)
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
