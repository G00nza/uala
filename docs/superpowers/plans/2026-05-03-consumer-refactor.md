# Consumer Refactor — Consumers as Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor RabbitMQ consumers to be pure infrastructure (unmarshal → call UC → ack/nack), moving all business logic into use cases, one consumer file per channel.

**Architecture:** Introduce `AppendTweetToTimelineUseCase` as the atomic write primitive, shared by `FanoutTweetUseCase` and `BackfillTimelineUseCase`. Each of the four channels gets its own consumer struct; `runLoop` is extracted to a package-level function. The monolithic `consumer.go` is deleted.

**Tech Stack:** Go stdlib, `golang.org/x/sync/errgroup`, `github.com/rabbitmq/amqp091-go`, `github.com/google/uuid`

---

## File Map

| Action | File | Responsibility |
|---|---|---|
| Create | `internal/usecase/append_tweet_to_timeline.go` | Atomic: write one tweet to one user's timeline |
| Create | `internal/usecase/append_tweet_to_timeline_test.go` | Unit tests for above |
| Create | `internal/usecase/fanout_tweet.go` | Fetch followers + concurrent fan-out a TweetCreatedEvent |
| Create | `internal/usecase/fanout_tweet_test.go` | Unit tests (migrated from consumer_test.go) |
| Create | `internal/usecase/backfill_timeline.go` | Fetch followee tweets + append to follower's timeline |
| Create | `internal/usecase/backfill_timeline_test.go` | Unit tests |
| Modify | `internal/usecase/mocks_test.go` | Add mockTimelineFanout, mockFanoutRetryPublisher, mockUserTweetsRepo; extend mockFollowRepo |
| Create | `internal/messaging/rabbitmq/loop.go` | Package-level `runLoop` function (extracted from consumer.go) |
| Create | `internal/messaging/rabbitmq/user_activity_consumer.go` | Consumes `user.activity`, inline UpdateLastActive |
| Create | `internal/messaging/rabbitmq/user_activity_consumer_test.go` | ack/nack unit tests |
| Create | `internal/messaging/rabbitmq/fanout_retry_consumer.go` | Consumes `fanout.retry`, dead-letter logic + calls AppendTweetToTimelineUseCase |
| Create | `internal/messaging/rabbitmq/fanout_retry_consumer_test.go` | ack/nack/dead-letter unit tests |
| Create | `internal/messaging/rabbitmq/tweet_created_consumer.go` | Consumes `tweet.created`, calls FanoutTweetUseCase |
| Create | `internal/messaging/rabbitmq/tweet_created_consumer_test.go` | ack/nack unit tests |
| Create | `internal/messaging/rabbitmq/follow_created_consumer.go` | Consumes `follow.created`, calls BackfillTimelineUseCase |
| Create | `internal/messaging/rabbitmq/follow_created_consumer_test.go` | ack/nack unit tests |
| Delete | `internal/messaging/rabbitmq/consumer.go` | Replaced by the four files above + loop.go |
| Delete | `internal/messaging/rabbitmq/consumer_test.go` | Tests migrated to UC test files |
| Modify | `cmd/api/main.go` | Replace NewConsumer wiring with four individual consumer constructors |

---

## Task 1: Extend usecase/mocks_test.go with new mocks

**Files:**
- Modify: `internal/usecase/mocks_test.go`

Add the following types to the file. They are needed by Tasks 2, 3, and 4.

- [ ] **Step 1: Add mocks**

Append to `internal/usecase/mocks_test.go`:

```go
// --- TimelineFanout mock ---

type mockTimelineFanoutCall struct {
	userID uuid.UUID
	item   domain.TweetItem
	ttl    time.Duration
}

type mockTimelineFanout struct {
	mu    sync.Mutex
	calls []mockTimelineFanoutCall
	err   error
}

func (m *mockTimelineFanout) AppendTweet(_ context.Context, userID uuid.UUID, item domain.TweetItem, ttl time.Duration) error {
	m.mu.Lock()
	m.calls = append(m.calls, mockTimelineFanoutCall{userID: userID, item: item, ttl: ttl})
	m.mu.Unlock()
	return m.err
}

// --- FanoutRetryPublisher mock ---

type mockFanoutRetryPublisher struct {
	mu     sync.Mutex
	events []domain.FanoutRetryEvent
	err    error
}

func (m *mockFanoutRetryPublisher) PublishFanoutRetry(_ context.Context, evt domain.FanoutRetryEvent) error {
	m.mu.Lock()
	m.events = append(m.events, evt)
	m.mu.Unlock()
	return m.err
}

// --- UserTweetsRepository mock ---

type mockUserTweetsRepo struct {
	tweets []domain.TweetItem
	err    error
}

func (m *mockUserTweetsRepo) GetLatestByUser(_ context.Context, _ uuid.UUID, _ int) ([]domain.TweetItem, error) {
	return m.tweets, m.err
}
```

Also update the existing `mockFollowRepo` to support configurable `GetActiveFollowers`:

Replace the existing `mockFollowRepo` struct and its methods with:

```go
type mockFollowRepo struct {
	existsResult    bool
	existsErr       error
	createErr       error
	followers       []uuid.UUID
	getFollowersErr error
	activeFollowers    []domain.FollowerActivity
	activeFollowersErr error
}

func (m *mockFollowRepo) Create(ctx context.Context, f *domain.Follow) error {
	return m.createErr
}

func (m *mockFollowRepo) Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error) {
	return m.existsResult, m.existsErr
}

func (m *mockFollowRepo) GetFollowers(ctx context.Context, followeeID uuid.UUID) ([]uuid.UUID, error) {
	return m.followers, m.getFollowersErr
}

func (m *mockFollowRepo) GetActiveFollowers(_ context.Context, _ uuid.UUID, _ time.Time) ([]domain.FollowerActivity, error) {
	return m.activeFollowers, m.activeFollowersErr
}
```

Add `"sync"` to the imports of `mocks_test.go`.

- [ ] **Step 2: Verify tests still compile**

```bash
go test ./internal/usecase/... 
```

Expected: all existing tests pass (PASS, no compilation errors).

- [ ] **Step 3: Commit**

```bash
git add internal/usecase/mocks_test.go
git commit -m "test(usecase): extend mocks for consumer refactor"
```

---

## Task 2: AppendTweetToTimelineUseCase

**Files:**
- Create: `internal/usecase/append_tweet_to_timeline.go`
- Create: `internal/usecase/append_tweet_to_timeline_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/usecase/append_tweet_to_timeline_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/usecase/... -run TestAppendTweetToTimeline -v
```

Expected: FAIL with `undefined: usecase.NewAppendTweetToTimelineUseCase`.

- [ ] **Step 3: Implement the use case**

Create `internal/usecase/append_tweet_to_timeline.go`:

```go
package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type AppendTweetToTimelineUseCase struct {
	fanout domain.TimelineFanout
}

func NewAppendTweetToTimelineUseCase(fanout domain.TimelineFanout) *AppendTweetToTimelineUseCase {
	return &AppendTweetToTimelineUseCase{fanout: fanout}
}

func (uc *AppendTweetToTimelineUseCase) Execute(ctx context.Context, followerID uuid.UUID, tweet domain.TweetItem, ttl time.Duration) error {
	return uc.fanout.AppendTweet(ctx, followerID, tweet, ttl)
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/usecase/... -run TestAppendTweetToTimeline -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/usecase/append_tweet_to_timeline.go internal/usecase/append_tweet_to_timeline_test.go
git commit -m "feat(usecase): add AppendTweetToTimelineUseCase"
```

---

## Task 3: FanoutTweetUseCase

**Files:**
- Create: `internal/usecase/fanout_tweet.go`
- Create: `internal/usecase/fanout_tweet_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/usecase/fanout_tweet_test.go`:

```go
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
	if len(retryPub.events) != 1 {
		t.Fatalf("want 1 retry event, got %d", len(retryPub.events))
	}
	if retryPub.events[0].FollowerID != followerID {
		t.Errorf("wrong followerID in retry event")
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

func TestFanoutTweet_AllExpiredFollowers_ReturnsNil(t *testing.T) {
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

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()}
	err := uc.Execute(context.Background(), evt)

	if err != nil {
		t.Errorf("expected nil for all-expired followers, got %v", err)
	}
	trackFanout.mu.Lock()
	defer trackFanout.mu.Unlock()
	if len(trackFanout.calls) != 0 {
		t.Errorf("expected no AppendTweet calls for expired followers, got %d", len(trackFanout.calls))
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/usecase/... -run TestFanoutTweet -v
```

Expected: FAIL with `undefined: usecase.FanoutTweetUseCase`.

- [ ] **Step 3: Implement FanoutTweetUseCase**

Create `internal/usecase/fanout_tweet.go`:

```go
package usecase

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"uala/internal/domain"
)

const defaultFanoutWorkers = 100

type FanoutTweetUseCase struct {
	followRepo     domain.FollowRepository
	appendUC       *AppendTweetToTimelineUseCase
	retryPublisher domain.FanoutRetryPublisher
	activityTTL    time.Duration
	fanoutWorkers  int
}

func NewFanoutTweetUseCase(
	followRepo domain.FollowRepository,
	appendUC *AppendTweetToTimelineUseCase,
	activityTTL time.Duration,
) *FanoutTweetUseCase {
	return &FanoutTweetUseCase{
		followRepo:    followRepo,
		appendUC:      appendUC,
		activityTTL:   activityTTL,
		fanoutWorkers: defaultFanoutWorkers,
	}
}

func (uc *FanoutTweetUseCase) WithRetryPublisher(p domain.FanoutRetryPublisher) *FanoutTweetUseCase {
	uc.retryPublisher = p
	return uc
}

func (uc *FanoutTweetUseCase) WithFanoutWorkers(n int) *FanoutTweetUseCase {
	uc.fanoutWorkers = n
	return uc
}

func (uc *FanoutTweetUseCase) Execute(ctx context.Context, evt domain.TweetCreatedEvent) error {
	activeSince := time.Now().Add(-uc.activityTTL)
	followers, err := uc.followRepo.GetActiveFollowers(ctx, evt.UserID, activeSince)
	if err != nil {
		return err
	}
	followers = append([]domain.FollowerActivity{{ID: evt.UserID, LastActive: time.Now()}}, followers...)

	item := domain.TweetItem{
		ID:        evt.TweetID,
		UserID:    evt.UserID,
		Username:  evt.Username,
		Content:   evt.Content,
		CreatedAt: evt.CreatedAt,
	}
	return uc.fanoutToFollowers(ctx, item, followers)
}

func (uc *FanoutTweetUseCase) fanoutToFollowers(ctx context.Context, item domain.TweetItem, followers []domain.FollowerActivity) error {
	var (
		sem      = make(chan struct{}, uc.fanoutWorkers)
		g, gctx  = errgroup.WithContext(ctx)
		handled  atomic.Int64
		eligible atomic.Int64
	)

	for _, fa := range followers {
		remaining := uc.activityTTL - time.Since(fa.LastActive)
		if remaining <= 0 {
			continue
		}
		eligible.Add(1)
		fid := fa.ID
		ttl := remaining
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			if err := uc.appendUC.Execute(gctx, fid, item, ttl); err != nil {
				slog.ErrorContext(gctx, "fanout: append tweet", "follower_id", fid, "err", err)
				if uc.retryPublisher != nil {
					if pubErr := uc.retryPublisher.PublishFanoutRetry(ctx, domain.FanoutRetryEvent{
						FollowerID: fid,
						Tweet:      item,
					}); pubErr != nil {
						slog.ErrorContext(ctx, "fanout: publish retry", "follower_id", fid, "err", pubErr)
						return nil
					}
					handled.Add(1)
				}
				return nil
			}
			handled.Add(1)
			return nil
		})
	}

	_ = g.Wait()

	if eligible.Load() > 0 && handled.Load() == 0 {
		return errors.New("all fanout writes failed and no retries enqueued")
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/usecase/... -run TestFanoutTweet -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/usecase/fanout_tweet.go internal/usecase/fanout_tweet_test.go
git commit -m "feat(usecase): add FanoutTweetUseCase"
```

---

## Task 4: BackfillTimelineUseCase

**Files:**
- Create: `internal/usecase/backfill_timeline.go`
- Create: `internal/usecase/backfill_timeline_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/usecase/backfill_timeline_test.go`:

```go
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
		usecase.NewAppendTweetToTimelineUseCase(fanout),
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
		usecase.NewAppendTweetToTimelineUseCase(&mockTimelineFanout{}),
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
		usecase.NewAppendTweetToTimelineUseCase(fanout),
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
		usecase.NewAppendTweetToTimelineUseCase(&mockTimelineFanout{err: errors.New("redis down")}),
		10,
		24*time.Hour,
	)

	err := uc.Execute(context.Background(), uuid.New(), uuid.New())

	if err != nil {
		t.Errorf("expected nil when individual appends fail, got %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/usecase/... -run TestBackfillTimeline -v
```

Expected: FAIL with `undefined: usecase.BackfillTimelineUseCase`.

- [ ] **Step 3: Implement BackfillTimelineUseCase**

Create `internal/usecase/backfill_timeline.go`:

```go
package usecase

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type BackfillTimelineUseCase struct {
	userTweetsRepo domain.UserTweetsRepository
	appendUC       *AppendTweetToTimelineUseCase
	backfillLimit  int
	activityTTL    time.Duration
}

func NewBackfillTimelineUseCase(
	userTweetsRepo domain.UserTweetsRepository,
	appendUC *AppendTweetToTimelineUseCase,
	backfillLimit int,
	activityTTL time.Duration,
) *BackfillTimelineUseCase {
	return &BackfillTimelineUseCase{
		userTweetsRepo: userTweetsRepo,
		appendUC:       appendUC,
		backfillLimit:  backfillLimit,
		activityTTL:    activityTTL,
	}
}

func (uc *BackfillTimelineUseCase) Execute(ctx context.Context, followerID, followeeID uuid.UUID) error {
	tweets, err := uc.userTweetsRepo.GetLatestByUser(ctx, followeeID, uc.backfillLimit)
	if err != nil {
		return err
	}
	for _, tweet := range tweets {
		if err := uc.appendUC.Execute(ctx, followerID, tweet, uc.activityTTL); err != nil {
			slog.ErrorContext(ctx, "backfill: append tweet", "follower_id", followerID, "tweet_id", tweet.ID, "err", err)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/usecase/... -run TestBackfillTimeline -v
```

Expected: all PASS.

- [ ] **Step 5: Run full usecase suite**

```bash
go test ./internal/usecase/...
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/usecase/backfill_timeline.go internal/usecase/backfill_timeline_test.go
git commit -m "feat(usecase): add BackfillTimelineUseCase"
```

---

## Task 5: Extract runLoop to loop.go

**Files:**
- Create: `internal/messaging/rabbitmq/loop.go`

- [ ] **Step 1: Create loop.go**

Create `internal/messaging/rabbitmq/loop.go` with the function extracted verbatim from `consumer.go`:

```go
package rabbitmq

import (
	"context"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func runLoop(ctx context.Context, conn channeler, queue string, handler func(context.Context, amqp.Delivery)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ch, err := conn.Channel()
		if err != nil {
			slog.ErrorContext(ctx, "amqp: open channel", "queue", queue, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
		if err != nil {
			ch.Close()
			slog.ErrorContext(ctx, "amqp: consume", "queue", queue, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		notifyClosed := ch.NotifyClose(make(chan *amqp.Error, 1))
	consume:
		for {
			select {
			case <-ctx.Done():
				ch.Close()
				return
			case <-notifyClosed:
				break consume
			case d, ok := <-msgs:
				if !ok {
					break consume
				}
				handler(ctx, d)
			}
		}
		ch.Close()
	}
}
```

- [ ] **Step 2: Verify the package still compiles**

```bash
go build ./internal/messaging/rabbitmq/...
```

Expected: success (consumer.go still has `runLoop` as a method; there is no conflict since package-level `runLoop` and method `(c *Consumer) runLoop` have different receivers — Go allows this).

- [ ] **Step 3: Commit**

```bash
git add internal/messaging/rabbitmq/loop.go
git commit -m "refactor(rabbitmq): extract runLoop to package-level function"
```

---

## Task 6: UserActivityConsumer

**Files:**
- Create: `internal/messaging/rabbitmq/user_activity_consumer.go`
- Create: `internal/messaging/rabbitmq/user_activity_consumer_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/messaging/rabbitmq/user_activity_consumer_test.go`:

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

type stubUserActivityRepo struct {
	mu     sync.Mutex
	calls  []domain.UserActivityEvent
	retErr error
}

func (s *stubUserActivityRepo) UpdateLastActive(_ context.Context, userID uuid.UUID, lastActive time.Time) error {
	s.mu.Lock()
	s.calls = append(s.calls, domain.UserActivityEvent{UserID: userID, LastActive: lastActive})
	s.mu.Unlock()
	return s.retErr
}

func TestUserActivityConsumer_AcksOnSuccess(t *testing.T) {
	repo := &stubUserActivityRepo{}
	c := &UserActivityConsumer{repo: repo}

	evt := domain.UserActivityEvent{UserID: uuid.New(), LastActive: time.Now()}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: body})

	if !acked {
		t.Error("expected ack on success")
	}
	if nacked {
		t.Error("expected no nack on success")
	}
}

func TestUserActivityConsumer_NacksOnRepoError(t *testing.T) {
	repo := &stubUserActivityRepo{retErr: errors.New("db error")}
	c := &UserActivityConsumer{repo: repo}

	evt := domain.UserActivityEvent{UserID: uuid.New(), LastActive: time.Now()}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: body})

	if acked {
		t.Error("expected no ack on repo error")
	}
	if !nacked {
		t.Error("expected nack on repo error")
	}
}

func TestUserActivityConsumer_NacksOnBadJSON(t *testing.T) {
	c := &UserActivityConsumer{repo: &stubUserActivityRepo{}}

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: []byte("bad-json")})

	if acked {
		t.Error("expected no ack on bad JSON")
	}
	if !nacked {
		t.Error("expected nack on bad JSON")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/messaging/rabbitmq/... -run TestUserActivityConsumer -v
```

Expected: FAIL with `undefined: UserActivityConsumer`.

- [ ] **Step 3: Implement UserActivityConsumer**

Create `internal/messaging/rabbitmq/user_activity_consumer.go`:

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

type UserActivityConsumer struct {
	conn channeler
	repo domain.UserActivityRepository
}

func NewUserActivityConsumer(conn channeler, repo domain.UserActivityRepository) *UserActivityConsumer {
	return &UserActivityConsumer{conn: conn, repo: repo}
}

func (c *UserActivityConsumer) Consume(ctx context.Context) {
	runLoop(ctx, c.conn, QueueUserActivity, func(ctx context.Context, d amqp.Delivery) {
		acked, nacked := c.handleDelivery(ctx, d)
		if acked {
			_ = d.Ack(false)
		} else if nacked {
			_ = d.Nack(false, false)
		}
	})
}

func (c *UserActivityConsumer) handleDelivery(ctx context.Context, d amqp.Delivery) (acked, nacked bool) {
	var evt domain.UserActivityEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal user activity", "err", err)
		return false, true
	}
	if err := c.repo.UpdateLastActive(ctx, evt.UserID, evt.LastActive); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: update last active", "user_id", evt.UserID, "err", err)
		return false, true
	}
	return true, false
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/messaging/rabbitmq/... -run TestUserActivityConsumer -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/messaging/rabbitmq/user_activity_consumer.go internal/messaging/rabbitmq/user_activity_consumer_test.go
git commit -m "feat(rabbitmq): add UserActivityConsumer"
```

---

## Task 7: FanoutRetryConsumer

**Files:**
- Create: `internal/messaging/rabbitmq/fanout_retry_consumer.go`
- Create: `internal/messaging/rabbitmq/fanout_retry_consumer_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/messaging/rabbitmq/fanout_retry_consumer_test.go`:

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

type stubTweetAppender struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (s *stubTweetAppender) Execute(_ context.Context, _ uuid.UUID, _ domain.TweetItem, _ time.Duration) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.err
}

type stubDeadLetterPub struct {
	mu     sync.Mutex
	events []domain.FanoutRetryEvent
	err    error
}

func (s *stubDeadLetterPub) PublishFanoutRetry(_ context.Context, evt domain.FanoutRetryEvent) error {
	s.mu.Lock()
	s.events = append(s.events, evt)
	s.mu.Unlock()
	return s.err
}

func makeFanoutRetryDelivery(body []byte, xDeathCount int64) amqp.Delivery {
	headers := amqp.Table{}
	if xDeathCount > 0 {
		headers["x-death"] = []interface{}{
			amqp.Table{"queue": QueueFanoutRetry, "count": xDeathCount},
		}
	}
	return amqp.Delivery{Body: body, Headers: headers}
}

func TestFanoutRetryConsumer_AcksOnSuccess(t *testing.T) {
	appender := &stubTweetAppender{}
	c := &FanoutRetryConsumer{svc: appender, activityTTL: 24 * time.Hour}

	evt := domain.FanoutRetryEvent{FollowerID: uuid.New(), Tweet: domain.TweetItem{ID: uuid.New(), CreatedAt: time.Now()}}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), makeFanoutRetryDelivery(body, 0))

	if !acked {
		t.Error("expected ack on success")
	}
	if nacked {
		t.Error("expected no nack on success")
	}
}

func TestFanoutRetryConsumer_NacksOnAppendFailure(t *testing.T) {
	appender := &stubTweetAppender{err: errors.New("redis down")}
	c := &FanoutRetryConsumer{svc: appender, activityTTL: 24 * time.Hour}

	evt := domain.FanoutRetryEvent{FollowerID: uuid.New(), Tweet: domain.TweetItem{ID: uuid.New(), CreatedAt: time.Now()}}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), makeFanoutRetryDelivery(body, 2))

	if acked {
		t.Error("expected no ack on failure")
	}
	if !nacked {
		t.Error("expected nack on failure")
	}
}

func TestFanoutRetryConsumer_DeadLettersAt10(t *testing.T) {
	dlq := &stubDeadLetterPub{}
	followerID := uuid.New()
	c := &FanoutRetryConsumer{
		svc:           &stubTweetAppender{err: errors.New("redis down")},
		deadLetterPub: dlq,
		activityTTL:   24 * time.Hour,
	}

	evt := domain.FanoutRetryEvent{FollowerID: followerID, Tweet: domain.TweetItem{ID: uuid.New(), CreatedAt: time.Now()}}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), makeFanoutRetryDelivery(body, 10))

	if !acked {
		t.Error("expected ack after dead-lettering")
	}
	if nacked {
		t.Error("expected no nack after dead-lettering")
	}
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	if len(dlq.events) != 1 {
		t.Fatalf("want 1 dead-letter event, got %d", len(dlq.events))
	}
	if dlq.events[0].FollowerID != followerID {
		t.Errorf("wrong followerID in dead-letter event")
	}
}

func TestFanoutRetryConsumer_NacksOnBadJSON(t *testing.T) {
	c := &FanoutRetryConsumer{svc: &stubTweetAppender{}, activityTTL: 24 * time.Hour}

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: []byte("bad-json")})

	if acked {
		t.Error("expected no ack on bad JSON")
	}
	if !nacked {
		t.Error("expected nack on bad JSON")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/messaging/rabbitmq/... -run TestFanoutRetryConsumer -v
```

Expected: FAIL with `undefined: FanoutRetryConsumer`.

- [ ] **Step 3: Implement FanoutRetryConsumer**

Create `internal/messaging/rabbitmq/fanout_retry_consumer.go`:

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
	"uala/internal/metrics"
)

const maxFanoutRetries = 10

type tweetAppender interface {
	Execute(ctx context.Context, followerID uuid.UUID, tweet domain.TweetItem, ttl time.Duration) error
}

type FanoutRetryConsumer struct {
	conn          channeler
	svc           tweetAppender
	deadLetterPub domain.FanoutRetryPublisher
	activityTTL   time.Duration
}

func NewFanoutRetryConsumer(
	conn channeler,
	svc tweetAppender,
	deadLetterPub domain.FanoutRetryPublisher,
	activityTTL time.Duration,
) *FanoutRetryConsumer {
	return &FanoutRetryConsumer{
		conn:          conn,
		svc:           svc,
		deadLetterPub: deadLetterPub,
		activityTTL:   activityTTL,
	}
}

func (c *FanoutRetryConsumer) Consume(ctx context.Context) {
	runLoop(ctx, c.conn, QueueFanoutRetry, func(ctx context.Context, d amqp.Delivery) {
		acked, nacked := c.handleDelivery(ctx, d)
		if acked {
			_ = d.Ack(false)
		} else if nacked {
			_ = d.Nack(false, false)
		}
	})
}

func (c *FanoutRetryConsumer) handleDelivery(ctx context.Context, d amqp.Delivery) (acked, nacked bool) {
	var evt domain.FanoutRetryEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal fanout retry", "err", err)
		return false, true
	}

	if xDeathCount(d) >= maxFanoutRetries {
		if c.deadLetterPub != nil {
			if err := c.deadLetterPub.PublishFanoutRetry(ctx, evt); err != nil {
				slog.ErrorContext(ctx, "rabbitmq: publish dead letter", "follower_id", evt.FollowerID, "err", err)
			}
		}
		metrics.FanoutDeadLetterTotal.WithLabelValues(evt.FollowerID.String()).Inc()
		slog.WarnContext(ctx, "rabbitmq: fanout dead letter", "follower_id", evt.FollowerID, "tweet_id", evt.Tweet.ID)
		return true, false
	}

	if err := c.svc.Execute(ctx, evt.FollowerID, evt.Tweet, c.activityTTL); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: retry append tweet", "follower_id", evt.FollowerID, "err", err)
		return false, true
	}
	return true, false
}

func xDeathCount(d amqp.Delivery) int64 {
	deaths, ok := d.Headers["x-death"].([]interface{})
	if !ok {
		return 0
	}
	for _, entry := range deaths {
		table, ok := entry.(amqp.Table)
		if !ok {
			continue
		}
		if table["queue"] == QueueFanoutRetry {
			if count, ok := table["count"].(int64); ok {
				return count
			}
		}
	}
	return 0
}
```

Add `"github.com/google/uuid"` to the imports of `fanout_retry_consumer.go`.

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/messaging/rabbitmq/... -run TestFanoutRetryConsumer -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/messaging/rabbitmq/fanout_retry_consumer.go internal/messaging/rabbitmq/fanout_retry_consumer_test.go
git commit -m "feat(rabbitmq): add FanoutRetryConsumer"
```

---

## Task 8: TweetCreatedConsumer

**Files:**
- Create: `internal/messaging/rabbitmq/tweet_created_consumer.go`
- Create: `internal/messaging/rabbitmq/tweet_created_consumer_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/messaging/rabbitmq/tweet_created_consumer_test.go`:

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

type stubTweetFanout struct {
	mu    sync.Mutex
	calls []domain.TweetCreatedEvent
	err   error
}

func (s *stubTweetFanout) Execute(_ context.Context, evt domain.TweetCreatedEvent) error {
	s.mu.Lock()
	s.calls = append(s.calls, evt)
	s.mu.Unlock()
	return s.err
}

func TestTweetCreatedConsumer_AcksOnSuccess(t *testing.T) {
	svc := &stubTweetFanout{}
	c := &TweetCreatedConsumer{svc: svc}

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), Content: "hi", CreatedAt: time.Now()}
	body, _ := json.Marshal(evt)

	c.handle(context.Background(), amqp.Delivery{Body: body})

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.calls) != 1 {
		t.Fatalf("want 1 Execute call, got %d", len(svc.calls))
	}
}

func TestTweetCreatedConsumer_NacksOnServiceError(t *testing.T) {
	svc := &stubTweetFanout{err: errors.New("fanout failed")}
	c := &TweetCreatedConsumer{svc: svc}

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()}
	body, _ := json.Marshal(evt)

	// handle calls d.Nack internally; we test indirectly via svc calls
	// Since amqp.Delivery.Nack on a zero-value delivery is a no-op panic-free call,
	// we just verify Execute was called and no panic occurred.
	c.handle(context.Background(), amqp.Delivery{Body: body})

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.calls) != 1 {
		t.Fatalf("want 1 Execute call even on error, got %d", len(svc.calls))
	}
}

func TestTweetCreatedConsumer_NacksOnBadJSON(t *testing.T) {
	svc := &stubTweetFanout{}
	c := &TweetCreatedConsumer{svc: svc}

	c.handle(context.Background(), amqp.Delivery{Body: []byte("not-json")})

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.calls) != 0 {
		t.Error("Execute must not be called on bad JSON")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/messaging/rabbitmq/... -run TestTweetCreatedConsumer -v
```

Expected: FAIL with `undefined: TweetCreatedConsumer`.

- [ ] **Step 3: Implement TweetCreatedConsumer**

Create `internal/messaging/rabbitmq/tweet_created_consumer.go`:

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
	"uala/internal/metrics"
)

type tweetFanout interface {
	Execute(ctx context.Context, evt domain.TweetCreatedEvent) error
}

type TweetCreatedConsumer struct {
	conn channeler
	svc  tweetFanout
}

func NewTweetCreatedConsumer(conn channeler, svc tweetFanout) *TweetCreatedConsumer {
	return &TweetCreatedConsumer{conn: conn, svc: svc}
}

func (c *TweetCreatedConsumer) Consume(ctx context.Context) {
	runLoop(ctx, c.conn, QueueTweetCreated, c.handle)
}

func (c *TweetCreatedConsumer) handle(ctx context.Context, d amqp.Delivery) {
	var evt domain.TweetCreatedEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal tweet event", "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		_ = d.Nack(false, false)
		return
	}

	start := time.Now()
	if err := c.svc.Execute(ctx, evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: fanout tweet", "tweet_id", evt.TweetID, "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		_ = d.Nack(false, true)
		return
	}

	metrics.FanoutDuration.Observe(time.Since(start).Seconds())
	metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueTweetCreated).Inc()
	_ = d.Ack(false)
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/messaging/rabbitmq/... -run TestTweetCreatedConsumer -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/messaging/rabbitmq/tweet_created_consumer.go internal/messaging/rabbitmq/tweet_created_consumer_test.go
git commit -m "feat(rabbitmq): add TweetCreatedConsumer"
```

---

## Task 9: FollowCreatedConsumer

**Files:**
- Create: `internal/messaging/rabbitmq/follow_created_consumer.go`
- Create: `internal/messaging/rabbitmq/follow_created_consumer_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/messaging/rabbitmq/follow_created_consumer_test.go`:

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

type stubTimelineBackfiller struct {
	mu    sync.Mutex
	calls []struct{ followerID, followeeID uuid.UUID }
	err   error
}

func (s *stubTimelineBackfiller) Execute(_ context.Context, followerID, followeeID uuid.UUID) error {
	s.mu.Lock()
	s.calls = append(s.calls, struct{ followerID, followeeID uuid.UUID }{followerID, followeeID})
	s.mu.Unlock()
	return s.err
}

func TestFollowCreatedConsumer_CallsBackfillWithCorrectIDs(t *testing.T) {
	svc := &stubTimelineBackfiller{}
	c := &FollowCreatedConsumer{svc: svc}

	followerID := uuid.New()
	followeeID := uuid.New()
	evt := domain.FollowCreatedEvent{FollowerID: followerID, FolloweeID: followeeID}
	body, _ := json.Marshal(evt)

	c.handle(context.Background(), amqp.Delivery{Body: body})

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.calls) != 1 {
		t.Fatalf("want 1 Execute call, got %d", len(svc.calls))
	}
	if svc.calls[0].followerID != followerID {
		t.Errorf("wrong followerID")
	}
	if svc.calls[0].followeeID != followeeID {
		t.Errorf("wrong followeeID")
	}
}

func TestFollowCreatedConsumer_NacksOnServiceError(t *testing.T) {
	svc := &stubTimelineBackfiller{err: errors.New("db down")}
	c := &FollowCreatedConsumer{svc: svc}

	evt := domain.FollowCreatedEvent{FollowerID: uuid.New(), FolloweeID: uuid.New()}
	body, _ := json.Marshal(evt)

	c.handle(context.Background(), amqp.Delivery{Body: body})

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.calls) != 1 {
		t.Fatalf("want 1 Execute call even on error, got %d", len(svc.calls))
	}
}

func TestFollowCreatedConsumer_NacksOnBadJSON(t *testing.T) {
	svc := &stubTimelineBackfiller{}
	c := &FollowCreatedConsumer{svc: svc}

	c.handle(context.Background(), amqp.Delivery{Body: []byte("bad-json")})

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.calls) != 0 {
		t.Error("Execute must not be called on bad JSON")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/messaging/rabbitmq/... -run TestFollowCreatedConsumer -v
```

Expected: FAIL with `undefined: FollowCreatedConsumer`.

- [ ] **Step 3: Implement FollowCreatedConsumer**

Create `internal/messaging/rabbitmq/follow_created_consumer.go`:

```go
package rabbitmq

import (
	"context"
	"encoding/json"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/google/uuid"
	"uala/internal/domain"
	"uala/internal/metrics"
)

type timelineBackfiller interface {
	Execute(ctx context.Context, followerID, followeeID uuid.UUID) error
}

type FollowCreatedConsumer struct {
	conn channeler
	svc  timelineBackfiller
}

func NewFollowCreatedConsumer(conn channeler, svc timelineBackfiller) *FollowCreatedConsumer {
	return &FollowCreatedConsumer{conn: conn, svc: svc}
}

func (c *FollowCreatedConsumer) Consume(ctx context.Context) {
	runLoop(ctx, c.conn, QueueFollowCreated, c.handle)
}

func (c *FollowCreatedConsumer) handle(ctx context.Context, d amqp.Delivery) {
	var evt domain.FollowCreatedEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal follow event", "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueFollowCreated).Inc()
		_ = d.Nack(false, false)
		return
	}

	if err := c.svc.Execute(ctx, evt.FollowerID, evt.FolloweeID); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: backfill timeline", "follower_id", evt.FollowerID, "followee_id", evt.FolloweeID, "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueFollowCreated).Inc()
		_ = d.Nack(false, true)
		return
	}

	metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueFollowCreated).Inc()
	_ = d.Ack(false)
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/messaging/rabbitmq/... -run TestFollowCreatedConsumer -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/messaging/rabbitmq/follow_created_consumer.go internal/messaging/rabbitmq/follow_created_consumer_test.go
git commit -m "feat(rabbitmq): add FollowCreatedConsumer"
```

---

## Task 10: Delete consumer.go and consumer_test.go

- [ ] **Step 1: Remove the old files**

```bash
rm internal/messaging/rabbitmq/consumer.go
rm internal/messaging/rabbitmq/consumer_test.go
```

- [ ] **Step 2: Verify the package still builds**

```bash
go build ./internal/messaging/rabbitmq/...
```

Expected: success. (`xDeathCount` was in `consumer.go` — it is now in `fanout_retry_consumer.go`. `maxFanoutRetries` is also there. `fanoutConcurrency` const is no longer needed.)

- [ ] **Step 3: Run all rabbitmq tests**

```bash
go test ./internal/messaging/rabbitmq/...
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add -u internal/messaging/rabbitmq/consumer.go internal/messaging/rabbitmq/consumer_test.go
git commit -m "refactor(rabbitmq): delete monolithic consumer.go"
```

---

## Task 11: Update cmd/api/main.go wiring

**Files:**
- Modify: `cmd/api/main.go`

- [ ] **Step 1: Replace consumer wiring**

In `cmd/api/main.go`, replace the block:

```go
consumer := rabbitmq.NewConsumer(amqpConn, followRepo, redisTimeline, pgTimelineRepo, cfg.FollowBackfillLimit).
    WithRetryPublisher(publisher).
    WithDeadLetterPublisher(rabbitmq.NewDeadLetterPublisher(publisher)).
    WithUserActivityRepo(userRepo).
    WithActivityTTL(cfg.ActivityTTL)

go consumer.ConsumeTweets(ctx)
go consumer.ConsumeFollows(ctx)
go consumer.ConsumeFanoutRetry(ctx)
go consumer.ConsumeUserActivity(ctx)
```

with:

```go
appendUC := usecase.NewAppendTweetToTimelineUseCase(redisTimeline)
fanoutUC := usecase.NewFanoutTweetUseCase(followRepo, appendUC, cfg.ActivityTTL).
    WithRetryPublisher(publisher)
backfillUC := usecase.NewBackfillTimelineUseCase(pgTimelineRepo, appendUC, cfg.FollowBackfillLimit, cfg.ActivityTTL)

go rabbitmq.NewTweetCreatedConsumer(amqpConn, fanoutUC).Consume(ctx)
go rabbitmq.NewFollowCreatedConsumer(amqpConn, backfillUC).Consume(ctx)
go rabbitmq.NewFanoutRetryConsumer(amqpConn, appendUC, rabbitmq.NewDeadLetterPublisher(publisher), cfg.ActivityTTL).Consume(ctx)
go rabbitmq.NewUserActivityConsumer(amqpConn, userRepo).Consume(ctx)
```

- [ ] **Step 2: Build to verify wiring compiles**

```bash
go build ./cmd/api/...
```

Expected: success with no errors.

- [ ] **Step 3: Run full test suite**

```bash
go test ./...
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add cmd/api/main.go
git commit -m "refactor(main): wire four individual consumers replacing monolithic Consumer"
```

---

## Verification

After all tasks are complete, run the integration suite to validate the full flow end-to-end:

```bash
INTEGRATION=1 go test ./...
```

Expected: all PASS.
