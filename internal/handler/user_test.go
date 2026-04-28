package handler_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/handler"
)

func TestUserHandler_Create_OK(t *testing.T) {
	id := uuid.New()
	svc := &mockUserSvc{user: &domain.User{ID: id, Username: "alice", CreatedAt: time.Now()}}
	h := handler.NewUserHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/users",
		bytes.NewBufferString(`{"username":"alice"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d", rec.Code)
	}
	var resp map[string]string
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp["id"] != id.String() {
		t.Fatalf("want %s, got %s", id, resp["id"])
	}
}

func TestUserHandler_Create_EmptyUsername(t *testing.T) {
	h := handler.NewUserHandler(&mockUserSvc{})

	req := httptest.NewRequest(http.MethodPost, "/users",
		bytes.NewBufferString(`{"username":""}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestUserHandler_Create_Conflict(t *testing.T) {
	svc := &mockUserSvc{err: domain.ErrUsernameConflict}
	h := handler.NewUserHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/users",
		bytes.NewBufferString(`{"username":"alice"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.Create(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d", rec.Code)
	}
}
