package usecase

import (
	"context"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type TimelineUseCase struct {
	userRepo     domain.UserRepository
	timelineRepo domain.TimelineRepository
}

func NewTimelineUseCase(userRepo domain.UserRepository, timelineRepo domain.TimelineRepository) *TimelineUseCase {
	return &TimelineUseCase{userRepo: userRepo, timelineRepo: timelineRepo}
}

func (uc *TimelineUseCase) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	if _, err := uc.userRepo.GetByID(ctx, userID); err != nil {
		return nil, err
	}
	return uc.timelineRepo.GetTimeline(ctx, userID)
}
