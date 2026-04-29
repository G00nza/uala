package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type FollowUseCase struct {
	userRepo   domain.UserRepository
	followRepo domain.FollowRepository
	publisher  domain.FollowEventPublisher
}

func NewFollowUseCase(userRepo domain.UserRepository, followRepo domain.FollowRepository, publisher domain.FollowEventPublisher) *FollowUseCase {
	return &FollowUseCase{userRepo: userRepo, followRepo: followRepo, publisher: publisher}
}

func (uc *FollowUseCase) Follow(ctx context.Context, followerID, followeeID uuid.UUID) error {
	if followerID == followeeID {
		return domain.ErrSelfFollow
	}
	exists, err := uc.followRepo.Exists(ctx, followerID, followeeID)
	if err != nil {
		return err
	}
	if exists {
		return domain.ErrAlreadyFollowing
	}
	if _, err := uc.userRepo.GetByID(ctx, followeeID); err != nil {
		return err
	}
	f := &domain.Follow{
		FollowerID: followerID,
		FolloweeID: followeeID,
		CreatedAt:  time.Now().UTC(),
	}
	if err := uc.followRepo.Create(ctx, f); err != nil {
		return err
	}
	evt := domain.FollowCreatedEvent{
		FollowerID: followerID,
		FolloweeID: followeeID,
	}
	_ = uc.publisher.PublishFollowCreated(ctx, evt)
	return nil
}
