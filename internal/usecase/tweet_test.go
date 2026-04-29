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
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID, Username: "alice"}}
	tweetRepo := &mockTweetRepo{}
	followRepo := &mockFollowRepo{}
	fanout := &mockTimelineFanout{}
	uc := usecase.NewTweetUseCase(userRepo, tweetRepo, followRepo, fanout)

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
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, &mockFollowRepo{}, &mockTimelineFanout{})

	_, err := uc.CreateTweet(context.Background(), uuid.New(), "hello")
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestTweetUseCase_CreateTweet_FanoutToFollowers(t *testing.T) {
	userID := uuid.New()
	follower1 := uuid.New()
	follower2 := uuid.New()

	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID, Username: "bob"}}
	followRepo := &mockFollowRepo{followers: []uuid.UUID{follower1, follower2}}
	fanout := &mockTimelineFanout{}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, followRepo, fanout)

	_, err := uc.CreateTweet(context.Background(), userID, "fanout this")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fanout.calls) != 2 {
		t.Fatalf("want 2 fanout calls, got %d", len(fanout.calls))
	}
	if fanout.calls[0].item.Content != "fanout this" {
		t.Fatalf("want content 'fanout this', got %s", fanout.calls[0].item.Content)
	}
	if fanout.calls[0].item.Username != "bob" {
		t.Fatalf("want username 'bob', got %s", fanout.calls[0].item.Username)
	}
}

func TestTweetUseCase_CreateTweet_FanoutErrorIsIgnored(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID, Username: "alice"}}
	followRepo := &mockFollowRepo{followers: []uuid.UUID{uuid.New()}}
	fanout := &mockTimelineFanout{appendErr: domain.ErrNotFound}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, followRepo, fanout)

	_, err := uc.CreateTweet(context.Background(), userID, "ignore fanout error")
	if err != nil {
		t.Fatalf("fanout error must not propagate, got: %v", err)
	}
}

func TestTweetUseCase_CreateTweet_NoFollowers_NoFanout(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID, Username: "loner"}}
	followRepo := &mockFollowRepo{followers: []uuid.UUID{}}
	fanout := &mockTimelineFanout{}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, followRepo, fanout)

	_, err := uc.CreateTweet(context.Background(), userID, "nobody follows me")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fanout.calls) != 0 {
		t.Fatalf("want 0 fanout calls, got %d", len(fanout.calls))
	}
}
