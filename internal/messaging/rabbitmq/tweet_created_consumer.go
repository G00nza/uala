package rabbitmq

import (
	"context"
	"encoding/json"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
	"uala/internal/metrics"
)

type fanoutTweetSvc interface {
	Execute(ctx context.Context, evt domain.TweetCreatedEvent) error
}

type TweetCreatedConsumer struct {
	conn channeler
	svc  fanoutTweetSvc
}

func NewTweetCreatedConsumer(conn channeler, svc fanoutTweetSvc) *TweetCreatedConsumer {
	return &TweetCreatedConsumer{conn: conn, svc: svc}
}

func (c *TweetCreatedConsumer) Consume(ctx context.Context) {
	runLoop(ctx, c.conn, QueueTweetCreated, func(ctx context.Context, d amqp.Delivery) {
		acked, nacked := c.handleDelivery(ctx, d)
		if acked {
			_ = d.Ack(false)
		} else if nacked {
			_ = d.Nack(false, false)
		}
	})
}

func (c *TweetCreatedConsumer) handleDelivery(ctx context.Context, d amqp.Delivery) (acked, nacked bool) {
	var evt domain.TweetCreatedEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal tweet event", "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		return false, true
	}

	if err := c.svc.Execute(ctx, evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: fanout tweet", "tweet_id", evt.TweetID, "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueTweetCreated).Inc()
		return false, true
	}

	metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueTweetCreated).Inc()
	return true, false
}
