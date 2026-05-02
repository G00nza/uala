package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
	"uala/internal/domain"
	"uala/internal/metrics"
)

const fanoutConcurrency = 100

const defaultActivityTTL = 24 * time.Hour

type Consumer struct {
	conn             channeler
	followRepo       domain.FollowRepository
	fanout           domain.TimelineFanout
	userTweetsRepo   domain.UserTweetsRepository
	userActivityRepo domain.UserActivityRepository
	backfillLimit    int
	fanoutWorkers    int
	retryPublisher   domain.FanoutRetryPublisher
	deadLetterPub    domain.FanoutRetryPublisher
	activityTTL      time.Duration
}

func NewConsumer(
	conn channeler,
	followRepo domain.FollowRepository,
	fanout domain.TimelineFanout,
	userTweetsRepo domain.UserTweetsRepository,
	backfillLimit int,
) *Consumer {
	return &Consumer{
		conn:           conn,
		followRepo:     followRepo,
		fanout:         fanout,
		userTweetsRepo: userTweetsRepo,
		backfillLimit:  backfillLimit,
		fanoutWorkers:  fanoutConcurrency,
		activityTTL:    defaultActivityTTL,
	}
}

func (c *Consumer) WithRetryPublisher(p domain.FanoutRetryPublisher) *Consumer {
	c.retryPublisher = p
	return c
}

func (c *Consumer) WithDeadLetterPublisher(p domain.FanoutRetryPublisher) *Consumer {
	c.deadLetterPub = p
	return c
}

func (c *Consumer) WithUserActivityRepo(r domain.UserActivityRepository) *Consumer {
	c.userActivityRepo = r
	return c
}

func (c *Consumer) WithActivityTTL(d time.Duration) *Consumer {
	c.activityTTL = d
	return c
}

func (c *Consumer) ConsumeTweets(ctx context.Context) {
	c.runLoop(ctx, QueueTweetCreated, c.handleTweetCreated)
}

func (c *Consumer) ConsumeFollows(ctx context.Context) {
	c.runLoop(ctx, QueueFollowCreated, c.handleFollowCreated)
}

func (c *Consumer) ConsumeUserActivity(ctx context.Context) {
	c.runLoop(ctx, QueueUserActivity, func(ctx context.Context, d amqp.Delivery) {
		acked, nacked := c.handleUserActivity(ctx, d)
		if acked {
			_ = d.Ack(false)
		} else if nacked {
			_ = d.Nack(false, false)
		}
	})
}

func (c *Consumer) ConsumeFanoutRetry(ctx context.Context) {
	c.runLoop(ctx, QueueFanoutRetry, func(ctx context.Context, d amqp.Delivery) {
		acked, nacked := c.handleFanoutRetry(ctx, d)
		if acked {
			_ = d.Ack(false)
		} else if nacked {
			_ = d.Nack(false, false)
		}
	})
}

// runLoop consumes messages from queue, restarting the channel on any error or drop.
func (c *Consumer) runLoop(ctx context.Context, queue string, handler func(context.Context, amqp.Delivery)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ch, err := c.conn.Channel()
		if err != nil {
			slog.ErrorContext(ctx, "amqp: open channel", "queue", queue, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
		if err != nil {
			ch.Close()
			slog.ErrorContext(ctx, "amqp: consume", "queue", queue, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		notifyClosed := ch.NotifyClose(make(chan *amqp.Error, 1))
	consume:
		for {
			select {
			case <-ctx.Done():
				ch.Close()
				return
			case <-notifyClosed:
				break consume
			case d, ok := <-msgs:
				if !ok {
					break consume
				}
				handler(ctx, d)
			}
		}
		ch.Close()
	}
}

func (c *Consumer) handleTweetCreated(ctx context.Context, d amqp.Delivery) {
	var evt domain.TweetCreatedEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal tweet event", "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		_ = d.Nack(false, false)
		return
	}

	start := time.Now()
	activeSince := time.Now().Add(-c.activityTTL)
	followers, err := c.followRepo.GetActiveFollowers(ctx, evt.UserID, activeSince)
	if err != nil {
		slog.ErrorContext(ctx, "rabbitmq: get active followers", "user_id", evt.UserID, "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		_ = d.Nack(false, true)
		return
	}

	if err := c.fanoutTweet(ctx, evt, followers); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: all fanout writes failed", "tweet_id", evt.TweetID, "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		_ = d.Nack(false, true)
		return
	}

	metrics.FanoutDuration.Observe(time.Since(start).Seconds())
	metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueTweetCreated).Inc()
	_ = d.Ack(false)
}

func (c *Consumer) fanoutTweet(ctx context.Context, evt domain.TweetCreatedEvent, followers []domain.FollowerActivity) error {
	item := domain.TweetItem{
		ID:        evt.TweetID,
		UserID:    evt.UserID,
		Username:  evt.Username,
		Content:   evt.Content,
		CreatedAt: evt.CreatedAt,
	}

	var (
		sem      = make(chan struct{}, c.fanoutWorkers)
		g, gctx  = errgroup.WithContext(ctx)
		handled  atomic.Int64
		eligible atomic.Int64
	)

	for _, fa := range followers {
		remaining := c.activityTTL - time.Since(fa.LastActive)
		if remaining <= 0 {
			continue
		}
		eligible.Add(1)
		fid := fa.ID
		ttl := remaining
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			if err := c.fanout.AppendTweet(gctx, fid, item, ttl); err != nil {
				slog.ErrorContext(gctx, "rabbitmq: fanout tweet", "follower_id", fid, "err", err)
				if c.retryPublisher != nil {
					if pubErr := c.retryPublisher.PublishFanoutRetry(ctx, domain.FanoutRetryEvent{
						FollowerID: fid,
						Tweet:      item,
					}); pubErr != nil {
						slog.ErrorContext(ctx, "rabbitmq: publish retry", "follower_id", fid, "err", pubErr)
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

const maxFanoutRetries = 10

func xDeathCount(d amqp.Delivery) int64 {
	deaths, ok := d.Headers["x-death"].([]interface{})
	if !ok {
		return 0
	}
	for _, entry := range deaths {
		table, ok := entry.(amqp.Table)
		if !ok {
			continue
		}
		if table["queue"] == QueueFanoutRetry {
			if count, ok := table["count"].(int64); ok {
				return count
			}
		}
	}
	return 0
}

func (c *Consumer) handleFanoutRetry(ctx context.Context, d amqp.Delivery) (acked, nacked bool) {
	var evt domain.FanoutRetryEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal fanout retry", "err", err)
		return false, true
	}

	if xDeathCount(d) >= maxFanoutRetries {
		if c.deadLetterPub != nil {
			if err := c.deadLetterPub.PublishFanoutRetry(ctx, evt); err != nil {
				slog.ErrorContext(ctx, "rabbitmq: publish dead letter", "follower_id", evt.FollowerID, "err", err)
			}
		}
		metrics.FanoutDeadLetterTotal.WithLabelValues(evt.FollowerID.String()).Inc()
		slog.WarnContext(ctx, "rabbitmq: fanout dead letter", "follower_id", evt.FollowerID, "tweet_id", evt.Tweet.ID)
		return true, false
	}

	if err := c.fanout.AppendTweet(ctx, evt.FollowerID, evt.Tweet, c.activityTTL); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: retry AppendTweet", "follower_id", evt.FollowerID, "err", err)
		return false, true
	}
	return true, false
}

func (c *Consumer) handleUserActivity(ctx context.Context, d amqp.Delivery) (acked, nacked bool) {
	var evt domain.UserActivityEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal user activity", "err", err)
		return false, true
	}
	if err := c.userActivityRepo.UpdateLastActive(ctx, evt.UserID, evt.LastActive); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: update last active", "user_id", evt.UserID, "err", err)
		return false, true
	}
	return true, false
}

func (c *Consumer) handleFollowCreated(ctx context.Context, d amqp.Delivery) {
	var evt domain.FollowCreatedEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal follow event", "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueFollowCreated).Inc()
		_ = d.Nack(false, false)
		return
	}

	tweets, err := c.userTweetsRepo.GetLatestByUser(ctx, evt.FolloweeID, c.backfillLimit)
	if err != nil {
		slog.ErrorContext(ctx, "rabbitmq: get tweets for backfill", "followee_id", evt.FolloweeID, "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueFollowCreated).Inc()
		_ = d.Nack(false, true)
		return
	}

	for _, tweet := range tweets {
		if err := c.fanout.AppendTweet(ctx, evt.FollowerID, tweet, c.activityTTL); err != nil {
			slog.ErrorContext(ctx, "rabbitmq: backfill tweet", "follower_id", evt.FollowerID, "err", err)
		}
	}

	metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueFollowCreated).Inc()
	_ = d.Ack(false)
}
