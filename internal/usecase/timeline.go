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

func (uc *TimelineUseCase) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	if _, err := uc.userRepo.GetByID(ctx, userID); err != nil {
		return nil, err
	}
	items, err := uc.timelineRepo.GetTimeline(ctx, userID)
	if err != nil {
		return nil, err
	}
	if uc.activityPub != nil {
		if pubErr := uc.activityPub.PublishUserActivity(ctx, domain.UserActivityEvent{
			UserID:     userID,
			LastActive: time.Now(),
		}); pubErr != nil {
			log.Printf("usecase: publish user activity for %s: %v", userID, pubErr)
		}
	}
	return items, nil
}
