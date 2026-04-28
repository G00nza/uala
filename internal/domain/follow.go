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
	ID        uuid.UUID
	UserID    uuid.UUID
	Username  string
	Content   string
	CreatedAt time.Time
}

type FollowRepository interface {
	Create(ctx context.Context, f *Follow) error
	Exists(ctx context.Context, followerID, followeeID uuid.UUID) (bool, error)
}

type TimelineRepository interface {
	GetTimeline(ctx context.Context, userID uuid.UUID) ([]TweetItem, error)
}
