package usecase

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type GetTimelineUseCase struct {
	userRepo     domain.UserRepository
	timelineRepo domain.TimelineRepository
	activityPub  domain.UserActivityPublisher
}

func NewGetTimelineUseCase(userRepo domain.UserRepository, timelineRepo domain.TimelineRepository) *GetTimelineUseCase {
	return &GetTimelineUseCase{userRepo: userRepo, timelineRepo: timelineRepo}
}

func (uc *GetTimelineUseCase) WithUserActivityPublisher(p domain.UserActivityPublisher) *GetTimelineUseCase {
	uc.activityPub = p
	return uc
}

func (uc *GetTimelineUseCase) Execute(ctx context.Context, userID uuid.UUID, after, before *uuid.UUID) ([]domain.TweetItem, error) {
	if _, err := uc.userRepo.GetByID(ctx, userID); err != nil {
		return nil, err
	}
	items, err := uc.timelineRepo.GetTimeline(ctx, domain.TimelineQuery{
		UserID: userID,
		After:  after,
		Before: before,
		Limit:  20,
	})
	if err != nil {
		return nil, err
	}
	if uc.activityPub != nil {
		evt := domain.UserActivityEvent{UserID: userID, LastActive: time.Now()}
		go func() {
			if pubErr := uc.activityPub.PublishUserActivity(context.Background(), evt); pubErr != nil {
				slog.Error("usecase: publish user activity", "user_id", userID, "err", pubErr)
			}
		}()
	}
	return items, nil
}
