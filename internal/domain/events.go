package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type TweetCreatedEvent struct {
	TweetID   uuid.UUID `json:"tweet_id"`
	UserID    uuid.UUID `json:"user_id"`
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type FollowCreatedEvent struct {
	FollowerID uuid.UUID `json:"follower_id"`
	FolloweeID uuid.UUID `json:"followee_id"`
}

type TweetEventPublisher interface {
	PublishTweetCreated(ctx context.Context, evt TweetCreatedEvent) error
}

type FollowEventPublisher interface {
	PublishFollowCreated(ctx context.Context, evt FollowCreatedEvent) error
}

type UserTweetsRepository interface {
	GetLatestByUser(ctx context.Context, userID uuid.UUID, limit int) ([]TweetItem, error)
}

type FanoutRetryEvent struct {
	FollowerID uuid.UUID `json:"follower_id"`
	Tweet      TweetItem `json:"tweet"`
}

type FanoutRetryPublisher interface {
	PublishFanoutRetry(ctx context.Context, evt FanoutRetryEvent) error
}
