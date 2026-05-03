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

func TestAppendTweetToTimeline_DelegatesToFanout(t *testing.T) {
	fanout := &mockTimelineFanout{}
	uc := usecase.NewAppendTweetToTimelineUseCase(fanout)

	followerID := uuid.New()
	tweet := domain.TweetItem{ID: uuid.New(), Content: "hello", CreatedAt: time.Now()}
	ttl := 10 * time.Minute

	err := uc.Execute(context.Background(), followerID, tweet, ttl)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fanout.mu.Lock()
	defer fanout.mu.Unlock()
	if len(fanout.calls) != 1 {
		t.Fatalf("want 1 AppendTweet call, got %d", len(fanout.calls))
	}
	if fanout.calls[0].userID != followerID {
		t.Errorf("wrong followerID: want %s got %s", followerID, fanout.calls[0].userID)
	}
	if fanout.calls[0].ttl != ttl {
		t.Errorf("wrong ttl: want %s got %s", ttl, fanout.calls[0].ttl)
	}
}

func TestAppendTweetToTimeline_PropagatesError(t *testing.T) {
	fanout := &mockTimelineFanout{err: errors.New("redis down")}
	uc := usecase.NewAppendTweetToTimelineUseCase(fanout)

	err := uc.Execute(context.Background(), uuid.New(), domain.TweetItem{CreatedAt: time.Now()}, time.Minute)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
