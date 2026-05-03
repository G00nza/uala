package rabbitmq

import (
	"context"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func runLoop(ctx context.Context, conn channeler, queue string, handler func(context.Context, amqp.Delivery)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ch, err := conn.Channel()
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
