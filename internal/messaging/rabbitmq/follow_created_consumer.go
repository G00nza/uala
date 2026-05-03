package rabbitmq

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
	"uala/internal/metrics"
)

type backfillSvc interface {
	Execute(ctx context.Context, followerID, followeeID uuid.UUID) error
}

type FollowCreatedConsumer struct {
	conn channeler
	svc  backfillSvc
}

func NewFollowCreatedConsumer(conn channeler, svc backfillSvc) *FollowCreatedConsumer {
	return &FollowCreatedConsumer{conn: conn, svc: svc}
}

func (c *FollowCreatedConsumer) Consume(ctx context.Context) {
	runLoop(ctx, c.conn, QueueFollowCreated, func(ctx context.Context, d amqp.Delivery) {
		acked, nacked := c.handleDelivery(ctx, d)
		if acked {
			_ = d.Ack(false)
		} else if nacked {
			_ = d.Nack(false, false)
		}
	})
}

func (c *FollowCreatedConsumer) handleDelivery(ctx context.Context, d amqp.Delivery) (acked, nacked bool) {
	var evt domain.FollowCreatedEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal follow event", "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueFollowCreated).Inc()
		return false, true
	}

	if err := c.svc.Execute(ctx, evt.FollowerID, evt.FolloweeID); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: backfill timeline", "follower_id", evt.FollowerID, "followee_id", evt.FolloweeID, "err", err)
		metrics.RabbitMQMessagesFailed.WithLabelValues(QueueFollowCreated).Inc()
		return false, true
	}

	metrics.RabbitMQMessagesProcessed.WithLabelValues(QueueFollowCreated).Inc()
	return true, false
}
