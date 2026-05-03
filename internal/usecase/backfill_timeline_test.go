package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

func TestBackfillTimeline_AppendsTweetsToFollower(t *testing.T) {
	followeeID := uuid.New()
	followerID := uuid.New()
	tweets := []domain.TweetItem{
		{ID: uuid.New(), Content: "first", CreatedAt: time.Now()},
		{ID: uuid.New(), Content: "second", CreatedAt: time.Now()},
	}

	fanout := &mockTimelineFanout{}
	uc := usecase.NewBackfillTimelineUseCase(
		&mockUserTweetsRepo{tweets: tweets},
		fanout,
		10,
		24*time.Hour,
	)

	err := uc.Execute(context.Background(), followerID, followeeID)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fanout.mu.Lock()
	defer fanout.mu.Unlock()
	if len(fanout.calls) != 2 {
		t.Fatalf("want 2 AppendTweet calls, got %d", len(fanout.calls))
	}
	for _, call := range fanout.calls {
		if call.userID != followerID {
			t.Errorf("want followerID %s, got %s", followerID, call.userID)
		}
	}
}

func TestBackfillTimeline_RepoError_ReturnsError(t *testing.T) {
	uc := usecase.NewBackfillTimelineUseCase(
		&mockUserTweetsRepo{err: errors.New("db down")},
		&mockTimelineFanout{},
		10,
		24*time.Hour,
	)

	err := uc.Execute(context.Background(), uuid.New(), uuid.New())

	if err == nil {
		t.Error("expected error from repo, got nil")
	}
}

func TestBackfillTimeline_EmptyTweets_NoAppendCalls(t *testing.T) {
	fanout := &mockTimelineFanout{}
	uc := usecase.NewBackfillTimelineUseCase(
		&mockUserTweetsRepo{tweets: nil},
		fanout,
		10,
		24*time.Hour,
	)

	err := uc.Execute(context.Background(), uuid.New(), uuid.New())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fanout.mu.Lock()
	defer fanout.mu.Unlock()
	if len(fanout.calls) != 0 {
		t.Errorf("want 0 AppendTweet calls, got %d", len(fanout.calls))
	}
}

func TestBackfillTimeline_AppendErrorDoesNotFail(t *testing.T) {
	tweets := []domain.TweetItem{{ID: uuid.New(), Content: "x", CreatedAt: time.Now()}}
	uc := usecase.NewBackfillTimelineUseCase(
		&mockUserTweetsRepo{tweets: tweets},
		&mockTimelineFanout{err: errors.New("redis down")},
		10,
		24*time.Hour,
	)

	err := uc.Execute(context.Background(), uuid.New(), uuid.New())

	if err != nil {
		t.Errorf("expected nil when individual appends fail, got %v", err)
	}
}
