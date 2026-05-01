package usecase_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestTweetUseCase_CreateTweet_OK(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID, Username: "alice"}}
	publisher := &mockTweetPublisher{}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, publisher)

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

func TestTweetUseCase_CreateTweet_EmptyContent(t *testing.T) {
	uc := usecase.NewTweetUseCase(&mockUserRepo{}, &mockTweetRepo{}, &mockTweetPublisher{})
	_, err := uc.CreateTweet(context.Background(), uuid.New(), "")
	if err != domain.ErrEmptyContent {
		t.Fatalf("want ErrEmptyContent, got %v", err)
	}
}

func TestTweetUseCase_CreateTweet_ContentTooLong(t *testing.T) {
	uc := usecase.NewTweetUseCase(&mockUserRepo{}, &mockTweetRepo{}, &mockTweetPublisher{})
	content := string(make([]rune, 281))
	_, err := uc.CreateTweet(context.Background(), uuid.New(), content)
	if err != domain.ErrContentTooLong {
		t.Fatalf("want ErrContentTooLong, got %v", err)
	}
}

func TestTweetUseCase_CreateTweet_UserNotFound(t *testing.T) {
	userRepo := &mockUserRepo{getErr: domain.ErrNotFound}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, &mockTweetPublisher{})

	_, err := uc.CreateTweet(context.Background(), uuid.New(), "hello")
	if err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestTweetUseCase_CreateTweet_PublishesEvent(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID, Username: "bob"}}
	publisher := &mockTweetPublisher{}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, publisher)

	_, err := uc.CreateTweet(context.Background(), userID, "async fanout")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("want 1 publish call, got %d", len(publisher.calls))
	}
	evt := publisher.calls[0]
	if evt.Content != "async fanout" {
		t.Fatalf("want content 'async fanout', got %s", evt.Content)
	}
	if evt.Username != "bob" {
		t.Fatalf("want username 'bob', got %s", evt.Username)
	}
	if evt.UserID != userID {
		t.Fatalf("want userID %s, got %s", userID, evt.UserID)
	}
}

func TestTweetUseCase_CreateTweet_PublishErrorIsIgnored(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID, Username: "alice"}}
	publisher := &mockTweetPublisher{publishErr: domain.ErrNotFound}
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, publisher)

	_, err := uc.CreateTweet(context.Background(), userID, "ignore publish error")
	if err != nil {
		t.Fatalf("publish error must not propagate, got: %v", err)
	}
}

func TestTweetUseCase_CreateTweet_PublishErrorIsLogged(t *testing.T) {
	userID := uuid.New()
	userRepo := &mockUserRepo{getUser: &domain.User{ID: userID, Username: "alice"}}
	publisher := &mockTweetPublisher{publishErr: errors.New("rabbitmq connection refused")}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	uc := usecase.NewTweetUseCase(userRepo, &mockTweetRepo{}, publisher).WithLogger(logger)

	_, err := uc.CreateTweet(context.Background(), userID, "test publish logging")
	if err != nil {
		t.Fatalf("publish error must not propagate, got: %v", err)
	}
	if !strings.Contains(buf.String(), "rabbitmq connection refused") {
		t.Fatalf("expected publish error in log, got: %q", buf.String())
	}
}
