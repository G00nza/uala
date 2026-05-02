package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Follow struct {
	FollowerID uuid.UUID
	FolloweeID uuid.UUID
	CreatedAt  time.Time
}

type FollowerActivity struct {
	ID         uuid.UUID
	LastActive time.Time
}

type TweetItem struct {
	ID        uuid.UUID `json:"id"`
	UserID    uuid.UUID `json:"user_id"`
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type FollowRepository interface {
	Create(ctx context.Context, f *Follow) error
	Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error)
	GetFollowers(ctx context.Context, followeeID uuid.UUID) ([]uuid.UUID, error)
	GetActiveFollowers(ctx context.Context, followeeID uuid.UUID, activeSince time.Time) ([]FollowerActivity, error)
}

type TimelineQuery struct {
	UserID uuid.UUID
	After  *uuid.UUID
	Before *uuid.UUID
	Limit  int
}

type TimelineRepository interface {
	GetTimeline(ctx context.Context, q TimelineQuery) ([]TweetItem, error)
}

type TimelineFanout interface {
	AppendTweet(ctx context.Context, userID uuid.UUID, item TweetItem, ttl time.Duration) error
}
