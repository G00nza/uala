package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type TweetUseCase struct {
	userRepo   domain.UserRepository
	tweetRepo  domain.TweetRepository
	followRepo domain.FollowRepository
	fanout     domain.TimelineFanout
}

func NewTweetUseCase(
	userRepo domain.UserRepository,
	tweetRepo domain.TweetRepository,
	followRepo domain.FollowRepository,
	fanout domain.TimelineFanout,
) *TweetUseCase {
	return &TweetUseCase{
		userRepo:   userRepo,
		tweetRepo:  tweetRepo,
		followRepo: followRepo,
		fanout:     fanout,
	}
}

func (uc *TweetUseCase) CreateTweet(ctx context.Context, userID uuid.UUID, content string) (*domain.Tweet, error) {
	user, err := uc.userRepo.GetByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	t := &domain.Tweet{
		ID:        uuid.New(),
		UserID:    userID,
		Content:   content,
		CreatedAt: time.Now().UTC(),
	}
	if err := uc.tweetRepo.Create(ctx, t); err != nil {
		return nil, err
	}
	followers, err := uc.followRepo.GetFollowers(ctx, userID)
	if err == nil && len(followers) > 0 {
		item := domain.TweetItem{
			ID:        t.ID,
			UserID:    t.UserID,
			Username:  user.Username,
			Content:   t.Content,
			CreatedAt: t.CreatedAt,
		}
		for _, followerID := range followers {
			_ = uc.fanout.AppendTweet(ctx, followerID, item)
		}
	}
	return t, nil
}
