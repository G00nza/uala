package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type TweetUseCase struct {
	userRepo  domain.UserRepository
	tweetRepo domain.TweetRepository
}

func NewTweetUseCase(userRepo domain.UserRepository, tweetRepo domain.TweetRepository) *TweetUseCase {
	return &TweetUseCase{userRepo: userRepo, tweetRepo: tweetRepo}
}

func (uc *TweetUseCase) CreateTweet(ctx context.Context, userID uuid.UUID, content string) (*domain.Tweet, error) {
	if _, err := uc.userRepo.GetByID(ctx, userID); err != nil {
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
	return t, nil
}
