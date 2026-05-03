package usecase_test

import (
	"context"
	"sync"
	"time"

	"uala/internal/domain"

	"github.com/google/uuid"
)

type mockUserRepo struct {
	createErr error
	getUser   *domain.User
	getErr    error
}

func (m *mockUserRepo) Create(ctx context.Context, u *domain.User) error {
	return m.createErr
}

func (m *mockUserRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.User, error) {
	return m.getUser, m.getErr
}

type mockTweetRepo struct {
	createErr error
}

func (m *mockTweetRepo) Create(ctx context.Context, t *domain.Tweet) error {
	return m.createErr
}

type mockFollowRepo struct {
	existsResult       bool
	existsErr          error
	createErr          error
	followers          []uuid.UUID
	getFollowersErr    error
	activeFollowers    []domain.FollowerActivity
	activeFollowersErr error
}

func (m *mockFollowRepo) Create(ctx context.Context, f *domain.Follow) error {
	return m.createErr
}

func (m *mockFollowRepo) Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error) {
	return m.existsResult, m.existsErr
}

func (m *mockFollowRepo) GetActiveFollowers(_ context.Context, _ uuid.UUID, _ time.Time) ([]domain.FollowerActivity, error) {
	return m.activeFollowers, m.activeFollowersErr
}

type mockTimelineRepo struct {
	items []domain.TweetItem
	err   error
}

func (m *mockTimelineRepo) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	return m.items, m.err
}

type mockTweetPublisher struct {
	publishErr error
	calls      []domain.TweetCreatedEvent
}

func (m *mockTweetPublisher) PublishTweetCreated(ctx context.Context, evt domain.TweetCreatedEvent) error {
	m.calls = append(m.calls, evt)
	return m.publishErr
}

type mockFollowPublisher struct {
	publishErr error
	calls      []domain.FollowCreatedEvent
}

func (m *mockFollowPublisher) PublishFollowCreated(ctx context.Context, evt domain.FollowCreatedEvent) error {
	m.calls = append(m.calls, evt)
	return m.publishErr
}

type mockUserActivityPublisher struct {
	publishErr error
	calls      []domain.UserActivityEvent
}

func (m *mockUserActivityPublisher) PublishUserActivity(ctx context.Context, evt domain.UserActivityEvent) error {
	m.calls = append(m.calls, evt)
	return m.publishErr
}

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
