package usecase

import (
	"context"
	"log/slog"
	"time"

	"uala/internal/domain"

	"github.com/google/uuid"
)

type FollowUserUseCase struct {
	userRepo   domain.UserRepository
	followRepo domain.FollowRepository
	publisher  domain.FollowEventPublisher
	logger     *slog.Logger
}

func NewFollowUserUseCase(userRepo domain.UserRepository, followRepo domain.FollowRepository, publisher domain.FollowEventPublisher) *FollowUserUseCase {
	return &FollowUserUseCase{userRepo: userRepo, followRepo: followRepo, publisher: publisher, logger: slog.Default()}
}

func (uc *FollowUserUseCase) WithLogger(l *slog.Logger) *FollowUserUseCase {
	uc.logger = l
	return uc
}

func (uc *FollowUserUseCase) Execute(ctx context.Context, followerID, followeeID uuid.UUID) error {
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
