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
)

func TestIntegration_User_Create(t *testing.T) {
	if testDB == nil {
		t.Skip("integration only")
	}
	truncate(t)
	srv := testServer(t)

	// Act
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/users",
		strings.NewReader(`{"username":"alice"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	defer resp.Body.Close()

	// Assert: response
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("want 201, got %d", resp.StatusCode)
	}
	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	id := result["id"]

	// Assert: DB state
	var username string
	err = testDB.QueryRow(context.Background(),
		"SELECT username FROM users WHERE id = $1", id).Scan(&username)
	if err != nil {
		t.Fatalf("user not found in DB: %v", err)
	}
	if username != "alice" {
		t.Errorf("want username 'alice', got %q", username)
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
