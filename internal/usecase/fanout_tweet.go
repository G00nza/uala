package usecase

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"uala/internal/domain"

	"github.com/google/uuid"

	"golang.org/x/sync/errgroup"
)

const defaultFanoutWorkers = 100

type FanoutTweetUseCase struct {
	followRepo     domain.FollowRepository
	fanout         domain.TimelineFanout
	retryPublisher domain.FanoutRetryPublisher
	activityTTL    time.Duration
	fanoutWorkers  int
}

func NewFanoutTweetUseCase(
	followRepo domain.FollowRepository,
	fanout domain.TimelineFanout,
	activityTTL time.Duration,
) *FanoutTweetUseCase {
	return &FanoutTweetUseCase{
		followRepo:    followRepo,
		fanout:        fanout,
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
		ttl := uc.remainingTTL(fa)
		if ttl <= 0 {
			continue
		}

		eligible.Add(1)
		fid, ttl := fa.ID, ttl
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			if uc.appendToTimeline(gctx, ctx, fid, item, ttl) {
				handled.Add(1)
			}
			return nil
		})
	}

	_ = g.Wait()

	if uc.allFanoutsFailed(eligible.Load(), handled.Load()) {
		return errors.New("all fanout writes failed and no retries enqueued")
	}
	return nil
}

func (uc *FanoutTweetUseCase) allFanoutsFailed(eligible, handled int64) bool {
	return eligible > 0 && handled == 0
}

func (uc *FanoutTweetUseCase) remainingTTL(fa domain.FollowerActivity) time.Duration {
	return uc.activityTTL - time.Since(fa.LastActive)
}

func (uc *FanoutTweetUseCase) appendToTimeline(gctx, ctx context.Context, followerID uuid.UUID, item domain.TweetItem, ttl time.Duration) (ok bool) {
	if err := uc.fanout.AppendTweet(gctx, followerID, item, ttl); err != nil {
		slog.ErrorContext(gctx, "fanout: append tweet", "follower_id", followerID, "err", err)
		return uc.enqueueRetry(ctx, followerID, item)
	}
	return true
}

func (uc *FanoutTweetUseCase) enqueueRetry(ctx context.Context, followerID uuid.UUID, item domain.TweetItem) (ok bool) {
	if uc.retryPublisher == nil {
		return false
	}
	if err := uc.retryPublisher.PublishFanoutRetry(ctx, domain.FanoutRetryEvent{
		FollowerID: followerID,
		Tweet:      item,
	}); err != nil {
		slog.ErrorContext(ctx, "fanout: publish retry", "follower_id", followerID, "err", err)
		return false
	}
	return true
}
