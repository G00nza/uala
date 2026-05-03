package handler_test

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type noopTweetPublisher struct{}

func (n *noopTweetPublisher) PublishTweetCreated(_ context.Context, _ domain.TweetCreatedEvent) error {
	return nil
}

type noopFollowPublisher struct{}

func (n *noopFollowPublisher) PublishFollowCreated(_ context.Context, _ domain.FollowCreatedEvent) error {
	return nil
}

type spyTweetPublisher struct {
	mu     sync.Mutex
	events []domain.TweetCreatedEvent
}

func (s *spyTweetPublisher) PublishTweetCreated(_ context.Context, evt domain.TweetCreatedEvent) error {
	s.mu.Lock()
	s.events = append(s.events, evt)
	s.mu.Unlock()
	return nil
}

type spyFollowPublisher struct {
	mu     sync.Mutex
	events []domain.FollowCreatedEvent
}

func (s *spyFollowPublisher) PublishFollowCreated(_ context.Context, evt domain.FollowCreatedEvent) error {
	s.mu.Lock()
	s.events = append(s.events, evt)
	s.mu.Unlock()
	return nil
}

type mockUserSvc struct {
	user *domain.User
	err  error
}

func (m *mockUserSvc) Execute(ctx context.Context, username string) (*domain.User, error) {
	return m.user, m.err
}

type mockTweetSvc struct {
	tweet *domain.Tweet
	err   error
}

func (m *mockTweetSvc) Execute(ctx context.Context, userID uuid.UUID, content string) (*domain.Tweet, error) {
	return m.tweet, m.err
}

type mockFollowSvc struct {
	err error
}

func (m *mockFollowSvc) Execute(ctx context.Context, followerID, followeeID uuid.UUID) error {
	return m.err
}

type mockTimelineSvc struct {
	items          []domain.TweetItem
	err            error
	capturedAfter  *uuid.UUID
	capturedBefore *uuid.UUID
}

func (m *mockTimelineSvc) Execute(ctx context.Context, userID uuid.UUID, after, before *uuid.UUID) ([]domain.TweetItem, error) {
	m.capturedAfter = after
	m.capturedBefore = before
	return m.items, m.err
}
