package rabbitmq

import (
	"context"
	"encoding/json"
	"log/slog"

	"uala/internal/domain"

	amqp "github.com/rabbitmq/amqp091-go"
)

type UserActivityConsumer struct {
	conn channeler
	repo domain.UserActivityRepository
}

func NewUserActivityConsumer(conn channeler, repo domain.UserActivityRepository) *UserActivityConsumer {
	return &UserActivityConsumer{conn: conn, repo: repo}
}

func (c *UserActivityConsumer) Consume(ctx context.Context) {
	runLoop(ctx, c.conn, QueueUserActivity, func(ctx context.Context, d amqp.Delivery) {
		acked, nacked := c.handleDelivery(ctx, d)
		if acked {
			_ = d.Ack(false)
		} else if nacked {
			_ = d.Nack(false, false)
		}
	})
}

func (c *UserActivityConsumer) handleDelivery(ctx context.Context, d amqp.Delivery) (acked, nacked bool) {
	var evt domain.UserActivityEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: unmarshal user activity", "err", err)
		return false, true
	}

	if err := c.repo.UpdateLastActive(ctx, evt.UserID, evt.LastActive); err != nil {
		slog.ErrorContext(ctx, "rabbitmq: update last active", "user_id", evt.UserID, "err", err)
		return false, true
	}

	return true, false
}
