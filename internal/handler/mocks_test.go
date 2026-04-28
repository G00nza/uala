package handler_test

import (
	"context"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type mockUserSvc struct {
	user *domain.User
	err  error
}

func (m *mockUserSvc) CreateUser(ctx context.Context, username string) (*domain.User, error) {
	return m.user, m.err
}

type mockTweetSvc struct {
	tweet *domain.Tweet
	err   error
}

func (m *mockTweetSvc) CreateTweet(ctx context.Context, userID uuid.UUID, content string) (*domain.Tweet, error) {
	return m.tweet, m.err
}

type mockFollowSvc struct {
	err error
}

func (m *mockFollowSvc) Follow(ctx context.Context, followerID, followeeID uuid.UUID) error {
	return m.err
}

type mockTimelineSvc struct {
	items []domain.TweetItem
	err   error
}

func (m *mockTimelineSvc) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	return m.items, m.err
}
