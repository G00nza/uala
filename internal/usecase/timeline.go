package usecase

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type TimelineUseCase struct {
	userRepo        domain.UserRepository
	timelineRepo    domain.TimelineRepository
	activityPub     domain.UserActivityPublisher
}

func NewTimelineUseCase(userRepo domain.UserRepository, timelineRepo domain.TimelineRepository) *TimelineUseCase {
	return &TimelineUseCase{userRepo: userRepo, timelineRepo: timelineRepo}
}

func (uc *TimelineUseCase) WithUserActivityPublisher(p domain.UserActivityPublisher) *TimelineUseCase {
	uc.activityPub = p
	return uc
}

func (uc *TimelineUseCase) GetTimeline(ctx context.Context, userID uuid.UUID, after, before *uuid.UUID) ([]domain.TweetItem, error) {
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
				log.Printf("usecase: publish user activity for %s: %v", userID, pubErr)
			}
		}()
	}
	return items, nil
}
