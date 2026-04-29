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
}

type TimelineRepository interface {
	GetTimeline(ctx context.Context, userID uuid.UUID) ([]TweetItem, error)
}

type TimelineFanout interface {
	AppendTweet(ctx context.Context, userID uuid.UUID, item TweetItem) error
}
