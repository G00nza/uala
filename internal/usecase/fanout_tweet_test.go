package usecase_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/usecase"
)

// delayedFanout tracks max concurrent AppendTweet calls.
type delayedFanout struct {
	mu          sync.Mutex
	active      int64
	maxObserved int64
}

func (f *delayedFanout) AppendTweet(_ context.Context, _ uuid.UUID, _ domain.TweetItem, _ time.Duration) error {
	cur := atomic.AddInt64(&f.active, 1)
	f.mu.Lock()
	if cur > f.maxObserved {
		f.maxObserved = cur
	}
	f.mu.Unlock()
	time.Sleep(5 * time.Millisecond)
	atomic.AddInt64(&f.active, -1)
	return nil
}

// errorFanout always fails.
type errorFanout struct{}

func (e *errorFanout) AppendTweet(_ context.Context, _ uuid.UUID, _ domain.TweetItem, _ time.Duration) error {
	return errors.New("redis unavailable")
}

func makeActiveFollowers(ids []uuid.UUID) []domain.FollowerActivity {
	result := make([]domain.FollowerActivity, len(ids))
	for i, id := range ids {
		result[i] = domain.FollowerActivity{ID: id, LastActive: time.Now()}
	}
	return result
}

func newFanoutUC(fanout domain.TimelineFanout, followRepo domain.FollowRepository) *usecase.FanoutTweetUseCase {
	appendUC := usecase.NewAppendTweetToTimelineUseCase(fanout)
	return usecase.NewFanoutTweetUseCase(followRepo, appendUC, 24*time.Hour)
}

func TestFanoutTweet_IsConcurrent(t *testing.T) {
	const numFollowers = 50
	ids := make([]uuid.UUID, numFollowers)
	for i := range ids {
		ids[i] = uuid.New()
	}

	fanout := &delayedFanout{}
	uc := newFanoutUC(fanout, &mockFollowRepo{activeFollowers: makeActiveFollowers(ids)})

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), Username: "u", Content: "hi", CreatedAt: time.Now()}
	if err := uc.Execute(context.Background(), evt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	fanout.mu.Lock()
	defer fanout.mu.Unlock()
	if fanout.maxObserved <= 1 {
		t.Errorf("expected concurrent fanout (max > 1), got %d", fanout.maxObserved)
	}
}

func TestFanoutTweet_IncludesAuthor(t *testing.T) {
	authorID := uuid.New()
	recorded := &mockTimelineFanout{}
	uc := newFanoutUC(recorded, &mockFollowRepo{})

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: authorID, CreatedAt: time.Now()}
	if err := uc.Execute(context.Background(), evt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	recorded.mu.Lock()
	defer recorded.mu.Unlock()
	if len(recorded.calls) != 1 {
		t.Fatalf("want 1 AppendTweet call (author), got %d", len(recorded.calls))
	}
	if recorded.calls[0].userID != authorID {
		t.Errorf("want authorID in fanout, got %s", recorded.calls[0].userID)
	}
}

func TestFanoutTweet_AllFail_ReturnsError(t *testing.T) {
	followers := makeActiveFollowers([]uuid.UUID{uuid.New(), uuid.New()})
	uc := newFanoutUC(&errorFanout{}, &mockFollowRepo{activeFollowers: followers})

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()}
	err := uc.Execute(context.Background(), evt)

	if err == nil {
		t.Error("expected error when all AppendTweet calls fail, got nil")
	}
}

func TestFanoutTweet_PublishesRetryOnFailure(t *testing.T) {
	followerID := uuid.New()
	retryPub := &mockFanoutRetryPublisher{}
	appendUC := usecase.NewAppendTweetToTimelineUseCase(&errorFanout{})
	uc := usecase.NewFanoutTweetUseCase(
		&mockFollowRepo{activeFollowers: makeActiveFollowers([]uuid.UUID{followerID})},
		appendUC,
		24*time.Hour,
	).WithRetryPublisher(retryPub)

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), Content: "hello", CreatedAt: time.Now()}
	_ = uc.Execute(context.Background(), evt)

	retryPub.mu.Lock()
	defer retryPub.mu.Unlock()
	// author + follower both fail → 2 retry events
	found := false
	for _, e := range retryPub.events {
		if e.FollowerID == followerID {
			found = true
		}
	}
	if !found {
		t.Errorf("expected retry event for followerID %s, not found in %v", followerID, retryPub.events)
	}
}

func TestFanoutTweet_RetryCountsAsHandled(t *testing.T) {
	followers := makeActiveFollowers([]uuid.UUID{uuid.New(), uuid.New()})
	appendUC := usecase.NewAppendTweetToTimelineUseCase(&errorFanout{})
	uc := usecase.NewFanoutTweetUseCase(
		&mockFollowRepo{activeFollowers: followers},
		appendUC,
		24*time.Hour,
	).WithRetryPublisher(&mockFanoutRetryPublisher{})

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()}
	err := uc.Execute(context.Background(), evt)

	if err != nil {
		t.Errorf("expected nil when retries published, got %v", err)
	}
}

func TestFanoutTweet_AllExpiredFollowers_OnlyAuthorReceivesTweet(t *testing.T) {
	authorID := uuid.New()
	expired := []domain.FollowerActivity{
		{ID: uuid.New(), LastActive: time.Now().Add(-25 * time.Hour)},
		{ID: uuid.New(), LastActive: time.Now().Add(-48 * time.Hour)},
	}
	trackFanout := &mockTimelineFanout{}
	uc := usecase.NewFanoutTweetUseCase(
		&mockFollowRepo{activeFollowers: expired},
		usecase.NewAppendTweetToTimelineUseCase(trackFanout),
		24*time.Hour,
	)

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: authorID, CreatedAt: time.Now()}
	err := uc.Execute(context.Background(), evt)

	if err != nil {
		t.Errorf("expected nil for all-expired followers, got %v", err)
	}
	trackFanout.mu.Lock()
	defer trackFanout.mu.Unlock()
	// Only the author (always active) should receive the tweet; expired followers are skipped.
	if len(trackFanout.calls) != 1 {
		t.Errorf("expected 1 AppendTweet call (author only), got %d", len(trackFanout.calls))
	}
	if len(trackFanout.calls) > 0 && trackFanout.calls[0].userID != authorID {
		t.Errorf("expected author to receive tweet, got %s", trackFanout.calls[0].userID)
	}
}

func TestFanoutTweet_RepoError_ReturnsError(t *testing.T) {
	uc := newFanoutUC(&mockTimelineFanout{}, &mockFollowRepo{activeFollowersErr: errors.New("db down")})

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()}
	err := uc.Execute(context.Background(), evt)

	if err == nil {
		t.Error("expected error when repo fails, got nil")
	}
}
