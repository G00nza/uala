package domain

import (
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

type FanoutRetryEvent struct {
	FollowerID uuid.UUID `json:"follower_id"`
	Tweet      TweetItem `json:"tweet"`
}

type UserActivityEvent struct {
	UserID     uuid.UUID `json:"user_id"`
	LastActive time.Time `json:"last_active"`
}
