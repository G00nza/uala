package handler_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func createUser(t *testing.T, srv *httptest.Server, username string) string {
	t.Helper()
	body := fmt.Sprintf(`{"username":"%s"}`, username)
	resp, err := http.Post(srv.URL+"/users", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("createUser: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("createUser: want 201, got %d", resp.StatusCode)
	}
	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	id := result["id"]
	if _, err := uuid.Parse(id); err != nil {
		t.Fatalf("createUser: invalid uuid in response: %s", id)
	}
	return id
}

func TestIntegration_User_Create(t *testing.T) {
	if testDB == nil {
		t.Skip("integration only")
	}
	truncate(t)
	srv := testServer(t)

	id := createUser(t, srv, "alice")

	// Verify write: same username returns 409
	resp, err := http.Post(srv.URL+"/users", "application/json",
		strings.NewReader(`{"username":"alice"}`))
	if err != nil {
		t.Fatalf("duplicate post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("want 409 on duplicate username, got %d", resp.StatusCode)
	}
	_ = id
}

func TestIntegration_Tweet_Create(t *testing.T) {
	if testDB == nil {
		t.Skip("integration only")
	}
	truncate(t)
	flushRedis(t)
	srv := testServer(t)

	userID := createUser(t, srv, "alice")

	body := `{"content":"hello world"}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/tweets", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", userID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create tweet: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("want 201, got %d", resp.StatusCode)
	}
	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	if _, err := uuid.Parse(result["id"]); err != nil {
		t.Fatalf("response id is not a valid uuid: %s", result["id"])
	}
}

func TestIntegration_Follow_And_Timeline(t *testing.T) {
	if testDB == nil {
		t.Skip("integration only")
	}
	truncate(t)
	flushRedis(t)
	srv := testServer(t)

	aliceID := createUser(t, srv, "alice")
	bobID := createUser(t, srv, "bob")

	// Alice tweets
	tweetReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/tweets",
		strings.NewReader(`{"content":"hello from alice"}`))
	tweetReq.Header.Set("Content-Type", "application/json")
	tweetReq.Header.Set("X-User-ID", aliceID)
	tweetResp, err := http.DefaultClient.Do(tweetReq)
	if err != nil {
		t.Fatalf("alice tweet: %v", err)
	}
	tweetResp.Body.Close()
	if tweetResp.StatusCode != http.StatusCreated {
		t.Fatalf("alice tweet: want 201, got %d", tweetResp.StatusCode)
	}

	// Bob follows Alice
	followBody := fmt.Sprintf(`{"followee_id":"%s"}`, aliceID)
	followReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/follow",
		strings.NewReader(followBody))
	followReq.Header.Set("Content-Type", "application/json")
	followReq.Header.Set("X-User-ID", bobID)
	followResp, err := http.DefaultClient.Do(followReq)
	if err != nil {
		t.Fatalf("bob follow: %v", err)
	}
	followResp.Body.Close()
	if followResp.StatusCode != http.StatusCreated {
		t.Fatalf("bob follow: want 201, got %d", followResp.StatusCode)
	}

	// Bob's timeline has Alice's tweet
	timelineReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/timeline", nil)
	timelineReq.Header.Set("X-User-ID", bobID)
	timelineResp, err := http.DefaultClient.Do(timelineReq)
	if err != nil {
		t.Fatalf("bob timeline: %v", err)
	}
	defer timelineResp.Body.Close()
	if timelineResp.StatusCode != http.StatusOK {
		t.Fatalf("bob timeline: want 200, got %d", timelineResp.StatusCode)
	}

	var result map[string]any
	json.NewDecoder(timelineResp.Body).Decode(&result)
	tweets, ok := result["tweets"].([]any)
	if !ok {
		t.Fatalf("expected tweets array in response")
	}
	if len(tweets) != 1 {
		t.Fatalf("want 1 tweet in bob's timeline, got %d", len(tweets))
	}
	tweet := tweets[0].(map[string]any)
	if tweet["content"] != "hello from alice" {
		t.Fatalf("want 'hello from alice', got %v", tweet["content"])
	}
	if tweet["username"] != "alice" {
		t.Fatalf("want username 'alice', got %v", tweet["username"])
	}
}
