package rabbitmq

import (
	"context"
	"encoding/json"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
	"uala/internal/metrics"
)

type Consumer struct {
	conn           *amqp.Connection
	followRepo     domain.FollowRepository
	fanout         domain.TimelineFanout
	userTweetsRepo domain.UserTweetsRepository
	backfillLimit  int
}

func NewConsumer(
	conn *amqp.Connection,
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
	}
}

func (c *Consumer) ConsumeTweets(ctx context.Context) {
	ch, msgs := c.openChannel(QueueTweetCreated)
	if ch == nil {
		return
	}
	defer ch.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-msgs:
			if !ok {
				return
			}
			c.handleTweetCreated(ctx, d)
		}
	}
}

func (c *Consumer) ConsumeFollows(ctx context.Context) {
	ch, msgs := c.openChannel(QueueFollowCreated)
	if ch == nil {
		return
	}
	defer ch.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-msgs:
			if !ok {
				return
			}
			c.handleFollowCreated(ctx, d)
		}
	}
}

func (c *Consumer) openChannel(queue string) (*amqp.Channel, <-chan amqp.Delivery) {
	ch, err := c.conn.Channel()
	if err != nil {
		log.Printf("rabbitmq: open channel for %s: %v", queue, err)
		return nil, nil
	}
	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		ch.Close()
		log.Printf("rabbitmq: consume %s: %v", queue, err)
		return nil, nil
	}
	return ch, msgs
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

	item := domain.TweetItem{
		ID:        evt.TweetID,
		UserID:    evt.UserID,
		Username:  evt.Username,
		Content:   evt.Content,
		CreatedAt: evt.CreatedAt,
	}
	for _, followerID := range followers {
		if err := c.fanout.AppendTweet(ctx, followerID, item); err != nil {
			log.Printf("rabbitmq: fanout tweet to %s: %v", followerID, err)
		}
	}

	metrics.FanoutDuration.Observe(time.Since(start).Seconds())
	metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueTweetCreated).Inc()
	_ = d.Ack(false)
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
