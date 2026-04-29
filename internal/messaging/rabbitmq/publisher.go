package rabbitmq

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

type Publisher struct {
	conn *amqp.Connection
}

func NewPublisher(conn *amqp.Connection) *Publisher {
	return &Publisher{conn: conn}
}

func (p *Publisher) PublishTweetCreated(ctx context.Context, evt domain.TweetCreatedEvent) error {
	return p.publish(QueueTweetCreated, evt)
}

func (p *Publisher) PublishFollowCreated(ctx context.Context, evt domain.FollowCreatedEvent) error {
	return p.publish(QueueFollowCreated, evt)
}

func (p *Publisher) publish(queue string, payload any) error {
	ch, err := p.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return ch.Publish("", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}
