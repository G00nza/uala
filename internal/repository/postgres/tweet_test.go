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
		CreatedAt: time.Now().UTC(),
	}
	if err := r.tweet.Create(context.Background(), tweet); err != nil {
		t.Fatalf("Create: %v", err)
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
