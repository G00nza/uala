package usecase

import (
	"context"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"uala/internal/domain"
)

type TweetUseCase struct {
	userRepo  domain.UserRepository
	tweetRepo domain.TweetRepository
	publisher domain.TweetEventPublisher
}

func NewTweetUseCase(
	userRepo domain.UserRepository,
	tweetRepo domain.TweetRepository,
	publisher domain.TweetEventPublisher,
) *TweetUseCase {
	return &TweetUseCase{
		userRepo:  userRepo,
		tweetRepo: tweetRepo,
		publisher: publisher,
	}
}

func (uc *TweetUseCase) CreateTweet(ctx context.Context, userID uuid.UUID, content string) (*domain.Tweet, error) {
	if content == "" {
		return nil, domain.ErrEmptyContent
	}
	if utf8.RuneCountInString(content) > 280 {
		return nil, domain.ErrContentTooLong
	}
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
	evt := domain.TweetCreatedEvent{
		TweetID:   t.ID,
		UserID:    t.UserID,
		Username:  user.Username,
		Content:   t.Content,
		CreatedAt: t.CreatedAt,
	}
	_ = uc.publisher.PublishTweetCreated(ctx, evt)
	return t, nil
}
