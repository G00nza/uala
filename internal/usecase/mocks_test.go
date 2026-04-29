package usecase_test

import (
	"context"

	"github.com/google/uuid"
	"uala/internal/domain"
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
	existsResult    bool
	existsErr       error
	createErr       error
	followers       []uuid.UUID
	getFollowersErr error
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

type mockTimelineRepo struct {
	items []domain.TweetItem
	err   error
}

func (m *mockTimelineRepo) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	return m.items, m.err
}

type mockTimelineFanout struct {
	appendErr error
	calls     []fanoutCall
}

type fanoutCall struct {
	userID uuid.UUID
	item   domain.TweetItem
}

func (m *mockTimelineFanout) AppendTweet(ctx context.Context, userID uuid.UUID, item domain.TweetItem) error {
	m.calls = append(m.calls, fanoutCall{userID: userID, item: item})
	return m.appendErr
}
