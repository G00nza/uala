package usecase

import (
	"context"
	"log/slog"
	"time"

	"uala/internal/domain"

	"github.com/google/uuid"
)

type BackfillTimelineUseCase struct {
	userTweetsRepo domain.UserTweetsRepository
	fanout         domain.TimelineFanout
	backfillLimit  int
	activityTTL    time.Duration
}

func NewBackfillTimelineUseCase(
	userTweetsRepo domain.UserTweetsRepository,
	fanout domain.TimelineFanout,
	backfillLimit int,
	activityTTL time.Duration,
) *BackfillTimelineUseCase {
	return &BackfillTimelineUseCase{
		userTweetsRepo: userTweetsRepo,
		fanout:         fanout,
		backfillLimit:  backfillLimit,
		activityTTL:    activityTTL,
	}
}

func (uc *BackfillTimelineUseCase) Execute(ctx context.Context, followerID, followeeID uuid.UUID) error {
	tweets, err := uc.userTweetsRepo.GetLatestByUser(ctx, followeeID, uc.backfillLimit)
	if err != nil {
		return err
	}

	for _, tweet := range tweets {
		if err := uc.fanout.AppendTweet(ctx, followerID, tweet, uc.activityTTL); err != nil {
			slog.ErrorContext(ctx, "backfill: append tweet", "follower_id", followerID, "tweet_id", tweet.ID, "err", err)
		}
	}

	return nil
}
