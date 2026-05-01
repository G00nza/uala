package usecase

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type FollowUseCase struct {
	userRepo   domain.UserRepository
	followRepo domain.FollowRepository
	publisher  domain.FollowEventPublisher
	logger     *slog.Logger
}

func NewFollowUseCase(userRepo domain.UserRepository, followRepo domain.FollowRepository, publisher domain.FollowEventPublisher) *FollowUseCase {
	return &FollowUseCase{userRepo: userRepo, followRepo: followRepo, publisher: publisher, logger: slog.Default()}
}

func (uc *FollowUseCase) WithLogger(l *slog.Logger) *FollowUseCase {
	uc.logger = l
	return uc
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
	if err := uc.publisher.PublishFollowCreated(ctx, evt); err != nil {
		uc.logger.ErrorContext(ctx, "publish follow event", "follower_id", followerID, "followee_id", followeeID, "err", err)
	}
	return nil
}
