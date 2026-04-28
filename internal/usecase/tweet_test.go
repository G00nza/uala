package usecase_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestTweetUseCase_CreateTweet_OK(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID}}
	tweetRepo := &mockTweetRepo{}
	uc := usecase.NewTweetUseCase(userRepo, tweetRepo)

	tweet, err := uc.CreateTweet(context.Background(), userID, "hello world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tweet.Content != "hello world" {
		t.Fatalf("want 'hello world', got %s", tweet.Content)
	}
	if tweet.ID == (uuid.UUID{}) {
		t.Fatal("ID must be set")
	}
}

func TestTweetUseCase_CreateTweet_UserNotFound(t *testing.T) {
	userRepo := &mockUserRepo{getErr: domain.ErrNotFound}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{})

	_, err := uc.CreateTweet(context.Background(), uuid.New(), "hello")
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
