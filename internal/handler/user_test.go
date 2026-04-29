package handler_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"uala/internal/domain"
	"uala/internal/handler"
)

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
