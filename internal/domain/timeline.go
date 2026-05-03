package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type TweetItem struct {
	ID        uuid.UUID `json:"id"`
	UserID    uuid.UUID `json:"user_id"`
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
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
