package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
	"uala/internal/domain"
	"uala/internal/metrics"
)

const fanoutConcurrency = 100

type Consumer struct {
	conn           channeler
	followRepo     domain.FollowRepository
	fanout         domain.TimelineFanout
	userTweetsRepo domain.UserTweetsRepository
	backfillLimit  int
	fanoutWorkers  int
	retryPublisher domain.FanoutRetryPublisher
	deadLetterPub  domain.FanoutRetryPublisher
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

func (c *Consumer) ConsumeTweets(ctx context.Context) {
	c.runLoop(ctx, QueueTweetCreated, c.handleTweetCreated)
}

func (c *Consumer) ConsumeFollows(ctx context.Context) {
	c.runLoop(ctx, QueueFollowCreated, c.handleFollowCreated)
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
			log.Printf("amqp: open channel for %s: %v, retrying in 1s", queue, err)
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
			log.Printf("amqp: consume %s: %v, retrying in 1s", queue, err)
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
		log.Printf("rabbitmq: unmarshal tweet event: %v", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		_ = d.Nack(false, false)
		return
	}

	start := time.Now()
	followers, err := c.followRepo.GetFollowers(ctx, evt.UserID)
	if err != nil {
		log.Printf("rabbitmq: get followers for %s: %v", evt.UserID, err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		_ = d.Nack(false, true)
		return
	}

	if err := c.fanoutTweet(ctx, evt, followers); err != nil {
		log.Printf("rabbitmq: all fanout writes failed for tweet %s: %v", evt.TweetID, err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		_ = d.Nack(false, true)
		return
	}

	metrics.FanoutDuration.Observe(time.Since(start).Seconds())
	metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueTweetCreated).Inc()
	_ = d.Ack(false)
}

func (c *Consumer) fanoutTweet(ctx context.Context, evt domain.TweetCreatedEvent, followers []uuid.UUID) error {
	item := domain.TweetItem{
		ID:        evt.TweetID,
		UserID:    evt.UserID,
		Username:  evt.Username,
		Content:   evt.Content,
		CreatedAt: evt.CreatedAt,
	}

	var (
		sem     = make(chan struct{}, c.fanoutWorkers)
		g, gctx = errgroup.WithContext(ctx)
		handled atomic.Int64
	)

	for _, followerID := range followers {
		fid := followerID
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			if err := c.fanout.AppendTweet(gctx, fid, item); err != nil {
				log.Printf("rabbitmq: fanout tweet to %s: %v", fid, err)
				if c.retryPublisher != nil {
					if pubErr := c.retryPublisher.PublishFanoutRetry(ctx, domain.FanoutRetryEvent{
						FollowerID: fid,
						Tweet:      item,
					}); pubErr != nil {
						log.Printf("rabbitmq: publish retry for %s: %v", fid, pubErr)
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

	if len(followers) > 0 && handled.Load() == 0 {
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
		log.Printf("rabbitmq: unmarshal fanout retry: %v", err)
		return false, true
	}

	if xDeathCount(d) >= maxFanoutRetries {
		if c.deadLetterPub != nil {
			if err := c.deadLetterPub.PublishFanoutRetry(ctx, evt); err != nil {
				log.Printf("rabbitmq: publish dead letter for %s: %v", evt.FollowerID, err)
			}
		}
		metrics.FanoutDeadLetterTotal.WithLabelValues(evt.FollowerID.String()).Inc()
		log.Printf("rabbitmq: fanout dead letter follower=%s tweet=%s", evt.FollowerID, evt.Tweet.ID)
		return true, false
	}

	if err := c.fanout.AppendTweet(ctx, evt.FollowerID, evt.Tweet); err != nil {
		log.Printf("rabbitmq: retry AppendTweet for %s: %v", evt.FollowerID, err)
		return false, true
	}
	return true, false
}

func (c *Consumer) handleFollowCreated(ctx context.Context, d amqp.Delivery) {
	var evt domain.FollowCreatedEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		log.Printf("rabbitmq: unmarshal follow event: %v", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueFollowCreated).Inc()
		_ = d.Nack(false, false)
		return
	}

	tweets, err := c.userTweetsRepo.GetLatestByUser(ctx, evt.FolloweeID, c.backfillLimit)
	if err != nil {
		log.Printf("rabbitmq: get tweets for backfill %s: %v", evt.FolloweeID, err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueFollowCreated).Inc()
		_ = d.Nack(false, true)
		return
	}

	for _, tweet := range tweets {
		if err := c.fanout.AppendTweet(ctx, evt.FollowerID, tweet); err != nil {
			log.Printf("rabbitmq: backfill tweet to %s: %v", evt.FollowerID, err)
		}
	}

	metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueFollowCreated).Inc()
	_ = d.Ack(false)
}
