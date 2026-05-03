package domain

import (
	"context"

	"github.com/google/uuid"
)

type TweetEventPublisher interface {
	PublishTweetCreated(ctx context.Context, evt TweetCreatedEvent) error
}

type FollowEventPublisher interface {
	PublishFollowCreated(ctx context.Context, evt FollowCreatedEvent) error
}

type UserTweetsRepository interface {
	GetLatestByUser(ctx context.Context, userID uuid.UUID, limit int) ([]TweetItem, error)
}

type FanoutRetryPublisher interface {
	PublishFanoutRetry(ctx context.Context, evt FanoutRetryEvent) error
}

type UserActivityPublisher interface {
	PublishUserActivity(ctx context.Context, evt UserActivityEvent) error
}
