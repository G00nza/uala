package usecase

import (
	"context"
	"log/slog"
	"time"
	"unicode/utf8"

	"uala/internal/domain"

	"github.com/google/uuid"
)

type CreateTweetUseCase struct {
	userRepo  domain.UserRepository
	tweetRepo domain.TweetRepository
	publisher domain.TweetEventPublisher
	logger    *slog.Logger
}

func NewCreateTweetUseCase(
	userRepo domain.UserRepository,
	tweetRepo domain.TweetRepository,
	publisher domain.TweetEventPublisher,
) *CreateTweetUseCase {
	return &CreateTweetUseCase{
		userRepo:  userRepo,
		tweetRepo: tweetRepo,
		publisher: publisher,
		logger:    slog.Default(),
	}
}

func (uc *CreateTweetUseCase) WithLogger(l *slog.Logger) *CreateTweetUseCase {
	uc.logger = l
	return uc
}

func (uc *CreateTweetUseCase) Execute(ctx context.Context, userID uuid.UUID, content string) (*domain.Tweet, error) {
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
	if err := uc.publisher.PublishTweetCreated(ctx, evt); err != nil {
		uc.logger.ErrorContext(ctx, "publish tweet event", "tweet_id", t.ID, "err", err)
	}
	return t, nil
}
