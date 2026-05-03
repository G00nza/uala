package usecase

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"uala/internal/domain"
)

const defaultFanoutWorkers = 100

type FanoutTweetUseCase struct {
	followRepo     domain.FollowRepository
	appendUC       *AppendTweetToTimelineUseCase
	retryPublisher domain.FanoutRetryPublisher
	activityTTL    time.Duration
	fanoutWorkers  int
}

func NewFanoutTweetUseCase(
	followRepo domain.FollowRepository,
	appendUC *AppendTweetToTimelineUseCase,
	activityTTL time.Duration,
) *FanoutTweetUseCase {
	return &FanoutTweetUseCase{
		followRepo:    followRepo,
		appendUC:      appendUC,
		activityTTL:   activityTTL,
		fanoutWorkers: defaultFanoutWorkers,
	}
}

func (uc *FanoutTweetUseCase) WithRetryPublisher(p domain.FanoutRetryPublisher) *FanoutTweetUseCase {
	uc.retryPublisher = p
	return uc
}

func (uc *FanoutTweetUseCase) WithFanoutWorkers(n int) *FanoutTweetUseCase {
	uc.fanoutWorkers = n
	return uc
}

func (uc *FanoutTweetUseCase) Execute(ctx context.Context, evt domain.TweetCreatedEvent) error {
	activeSince := time.Now().Add(-uc.activityTTL)
	followers, err := uc.followRepo.GetActiveFollowers(ctx, evt.UserID, activeSince)
	if err != nil {
		return err
	}
	followers = append([]domain.FollowerActivity{{ID: evt.UserID, LastActive: time.Now()}}, followers...)

	item := domain.TweetItem{
		ID:        evt.TweetID,
		UserID:    evt.UserID,
		Username:  evt.Username,
		Content:   evt.Content,
		CreatedAt: evt.CreatedAt,
	}
	return uc.fanoutToFollowers(ctx, item, followers)
}

func (uc *FanoutTweetUseCase) fanoutToFollowers(ctx context.Context, item domain.TweetItem, followers []domain.FollowerActivity) error {
	var (
		sem      = make(chan struct{}, uc.fanoutWorkers)
		g, gctx  = errgroup.WithContext(ctx)
		handled  atomic.Int64
		eligible atomic.Int64
	)

	for _, fa := range followers {
		remaining := uc.activityTTL - time.Since(fa.LastActive)
		if remaining <= 0 {
			continue
		}
		eligible.Add(1)
		fid := fa.ID
		ttl := remaining
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			if err := uc.appendUC.Execute(gctx, fid, item, ttl); err != nil {
				slog.ErrorContext(gctx, "fanout: append tweet", "follower_id", fid, "err", err)
				if uc.retryPublisher != nil {
					if pubErr := uc.retryPublisher.PublishFanoutRetry(ctx, domain.FanoutRetryEvent{
						FollowerID: fid,
						Tweet:      item,
					}); pubErr != nil {
						slog.ErrorContext(ctx, "fanout: publish retry", "follower_id", fid, "err", pubErr)
						return nil
					}
					handled.Add(1)
				}
				return nil
			}
			handled.Add(1)
			return nil
		})
	}

	_ = g.Wait()

	if eligible.Load() > 0 && handled.Load() == 0 {
		return errors.New("all fanout writes failed and no retries enqueued")
	}
	return nil
}

