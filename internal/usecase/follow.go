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
}

func NewFollowUseCase(userRepo domain.UserRepository, followRepo domain.FollowRepository) *FollowUseCase {
	return &FollowUseCase{userRepo: userRepo, followRepo: followRepo}
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
	return uc.followRepo.Create(ctx, f)
}
