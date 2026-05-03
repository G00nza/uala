package rabbitmq

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
	"uala/internal/metrics"
)

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

type tweetAppender interface {
	Execute(ctx context.Context, followerID uuid.UUID, tweet domain.TweetItem, ttl time.Duration) error
}

type FanoutRetryConsumer struct {
	conn          channeler
	svc           tweetAppender
	deadLetterPub domain.FanoutRetryPublisher
	activityTTL   time.Duration
}

func NewFanoutRetryConsumer(
	conn channeler,
	svc tweetAppender,
	deadLetterPub domain.FanoutRetryPublisher,
	activityTTL time.Duration,
) *FanoutRetryConsumer {
	return &FanoutRetryConsumer{
		conn:          conn,
		svc:           svc,
		deadLetterPub: deadLetterPub,
		activityTTL:   activityTTL,
	}
}

func (c *FanoutRetryConsumer) Consume(ctx context.Context) {
	runLoop(ctx, c.conn, QueueFanoutRetry, func(ctx context.Context, d amqp.Delivery) {
		acked, nacked := c.handleDelivery(ctx, d)
		if acked {
			_ = d.Ack(false)
		} else if nacked {
			_ = d.Nack(false, false)
		}
	})
}

func (c *FanoutRetryConsumer) handleDelivery(ctx context.Context, d amqp.Delivery) (acked, nacked bool) {
	var evt domain.FanoutRetryEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal fanout retry", "err", err)
		return false, true
	}

	if xDeathCount(d) >= maxFanoutRetries {
		if c.deadLetterPub != nil {
			if err := c.deadLetterPub.PublishFanoutRetry(ctx, evt); err != nil {
				slog.ErrorContext(ctx, "rabbitmq: publish dead-letter", "follower_id", evt.FollowerID, "err", err)
			}
		}
		metrics.FanoutDeadLetterTotal.WithLabelValues(evt.FollowerID.String()).Inc()
		return true, false
	}

	if err := c.svc.Execute(ctx, evt.FollowerID, evt.Tweet, c.activityTTL); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: fanout retry append", "follower_id", evt.FollowerID, "err", err)
		return false, true
	}
	return true, false
}
