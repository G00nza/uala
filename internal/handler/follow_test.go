package handler_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/handler"
)

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
