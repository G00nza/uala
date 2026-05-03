package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

func TestTweetRepository_Create(t *testing.T) {
	r := setup(t)

	user := &domain.User{ID: uuid.New(), Username: "alice", CreatedAt: time.Now().UTC()}
	_ = r.user.Create(context.Background(), user)

	tweet := &domain.Tweet{
		ID:        uuid.New(),
		UserID:    user.ID,
		Content:   "hello world",
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	if err := r.tweet.Create(context.Background(), tweet); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var gotUserID uuid.UUID
	var gotContent string
	var gotCreatedAt time.Time
	err := testDB.QueryRow(context.Background(),
		`SELECT user_id, content, created_at FROM tweets WHERE id = $1`, tweet.ID,
	).Scan(&gotUserID, &gotContent, &gotCreatedAt)
	if err != nil {
		t.Fatalf("read-back: %v", err)
	}
	if gotUserID != tweet.UserID {
		t.Errorf("want user_id %s, got %s", tweet.UserID, gotUserID)
	}
	if gotContent != tweet.Content {
		t.Errorf("want content %q, got %q", tweet.Content, gotContent)
	}
	if !gotCreatedAt.Equal(tweet.CreatedAt) {
		t.Errorf("want created_at %v, got %v", tweet.CreatedAt, gotCreatedAt)
	}
}

func TestTweetRepository_Create_UserNotFound(t *testing.T) {
	r := setup(t)

	tweet := &domain.Tweet{
		ID:        uuid.New(),
		UserID:    uuid.New(),
		Content:   "orphan tweet",
		CreatedAt: time.Now().UTC(),
	}
	err := r.tweet.Create(context.Background(), tweet)
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
