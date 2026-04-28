package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/repository/postgres"
)

func TestTweetRepository_Create(t *testing.T) {
	truncate(t)
	userRepo := postgres.NewUserRepository(testDB)
	tweetRepo := postgres.NewTweetRepository(testDB)

	user := &domain.User{ID: uuid.New(), Username: "alice", CreatedAt: time.Now().UTC()}
	_ = userRepo.Create(context.Background(), user)

	tweet := &domain.Tweet{
		ID:        uuid.New(),
		UserID:    user.ID,
		Content:   "hello world",
		CreatedAt: time.Now().UTC(),
	}
	if err := tweetRepo.Create(context.Background(), tweet); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestTweetRepository_Create_UserNotFound(t *testing.T) {
	truncate(t)
	tweetRepo := postgres.NewTweetRepository(testDB)

	tweet := &domain.Tweet{
		ID:        uuid.New(),
		UserID:    uuid.New(),
		Content:   "orphan tweet",
		CreatedAt: time.Now().UTC(),
	}
	err := tweetRepo.Create(context.Background(), tweet)
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
