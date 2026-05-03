package handler_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"uala/internal/domain"
	"uala/internal/handler"

	"github.com/google/uuid"
)

func TestIntegration_Follow_Create(t *testing.T) {
	if testDB == nil {
		t.Skip("integration only")
	}
	truncate(t)

	// Arrange
	aliceID := uuid.New()
	bobID := uuid.New()
	seedUser(t, aliceID, "alice")
	seedUser(t, bobID, "bob")
	pub := &spyFollowPublisher{}
	srv := testServerWith(t, &noopTweetPublisher{}, pub)

	// Act
	followBody := fmt.Sprintf(`{"followee_id":"%s"}`, aliceID)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/follow",
		strings.NewReader(followBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", bobID.String())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("follow: %v", err)
	}
	defer resp.Body.Close()

	// Assert: response
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("want 201, got %d", resp.StatusCode)
	}

	// Assert: DB state
	var count int
	err = testDB.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM follows WHERE follower_id = $1 AND followee_id = $2",
		bobID, aliceID).Scan(&count)
	if err != nil || count != 1 {
		t.Fatalf("follow not found in DB: %v", err)
	}

	// Assert: event published
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 1 {
		t.Fatalf("want 1 FollowCreatedEvent, got %d", len(pub.events))
	}
	if pub.events[0].FollowerID != bobID {
		t.Errorf("event FollowerID: want %s, got %s", bobID, pub.events[0].FollowerID)
	}
	if pub.events[0].FolloweeID != aliceID {
		t.Errorf("event FolloweeID: want %s, got %s", aliceID, pub.events[0].FolloweeID)
	}
}

func TestFollowHandler_Follow_MissingUserID(t *testing.T) {
	h := handler.NewFollowHandler(&mockFollowSvc{})

	req := httptest.NewRequest(http.MethodPost, "/follow",
		bytes.NewBufferString(`{"followee_id":"`+uuid.New().String()+`"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Follow(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestFollowHandler_Follow_SelfFollow(t *testing.T) {
	svc := &mockFollowSvc{err: domain.ErrSelfFollow}
	h := handler.NewFollowHandler(svc)

	id := uuid.New()
	body := fmt.Sprintf(`{"followee_id":"%s"}`, id)
	req := httptest.NewRequest(http.MethodPost, "/follow", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", id.String())
	rec := httptest.NewRecorder()

	h.Follow(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestFollowHandler_Follow_AlreadyFollowing(t *testing.T) {
	svc := &mockFollowSvc{err: domain.ErrAlreadyFollowing}
	h := handler.NewFollowHandler(svc)

	body := fmt.Sprintf(`{"followee_id":"%s"}`, uuid.New())
	req := httptest.NewRequest(http.MethodPost, "/follow", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.Follow(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d", rec.Code)
	}
}

func TestFollowHandler_Follow_FolloweeNotFound(t *testing.T) {
	svc := &mockFollowSvc{err: domain.ErrNotFound}
	h := handler.NewFollowHandler(svc)

	body := fmt.Sprintf(`{"followee_id":"%s"}`, uuid.New())
	req := httptest.NewRequest(http.MethodPost, "/follow", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", uuid.New().String())
	rec := httptest.NewRecorder()

	h.Follow(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rec.Code)
	}
}
