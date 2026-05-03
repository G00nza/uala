package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type AppendTweetToTimelineUseCase struct {
	fanout domain.TimelineFanout
}

func NewAppendTweetToTimelineUseCase(fanout domain.TimelineFanout) *AppendTweetToTimelineUseCase {
	return &AppendTweetToTimelineUseCase{fanout: fanout}
}

func (uc *AppendTweetToTimelineUseCase) Execute(ctx context.Context, followerID uuid.UUID, tweet domain.TweetItem, ttl time.Duration) error {
	return uc.fanout.AppendTweet(ctx, followerID, tweet, ttl)
}
