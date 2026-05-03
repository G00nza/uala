package usecase

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type BackfillTimelineUseCase struct {
	userTweetsRepo domain.UserTweetsRepository
	appendUC       *AppendTweetToTimelineUseCase
	backfillLimit  int
	activityTTL    time.Duration
}

func NewBackfillTimelineUseCase(
	userTweetsRepo domain.UserTweetsRepository,
	appendUC *AppendTweetToTimelineUseCase,
	backfillLimit int,
	activityTTL time.Duration,
) *BackfillTimelineUseCase {
	return &BackfillTimelineUseCase{
		userTweetsRepo: userTweetsRepo,
		appendUC:       appendUC,
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
		if err := uc.appendUC.Execute(ctx, followerID, tweet, uc.activityTTL); err != nil {
			slog.ErrorContext(ctx, "backfill: append tweet", "follower_id", followerID, "tweet_id", tweet.ID, "err", err)
		}
	}
	return nil
}
