package rabbitmq

import (
	"context"
	"encoding/json"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

// amqpChannel is the subset of *amqp.Channel the publisher uses.
type amqpChannel interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

type Publisher struct {
	openCh func() (amqpChannel, error)
	mu     sync.Mutex
	ch     amqpChannel
}

func NewPublisher(conn channeler) *Publisher {
	return &Publisher{
		openCh: func() (amqpChannel, error) { return conn.Channel() },
	}
}

func (p *Publisher) PublishTweetCreated(ctx context.Context, evt domain.TweetCreatedEvent) error {
	return p.publishToExchange("", QueueTweetCreated, evt)
}

func (p *Publisher) PublishFollowCreated(ctx context.Context, evt domain.FollowCreatedEvent) error {
	return p.publishToExchange("", QueueFollowCreated, evt)
}

func (p *Publisher) PublishUserActivity(ctx context.Context, evt domain.UserActivityEvent) error {
	return p.publishToExchange("", QueueUserActivity, evt)
}

func (p *Publisher) PublishFanoutRetry(ctx context.Context, evt domain.FanoutRetryEvent) error {
	return p.publishToExchange(ExchangeFanoutRetry, QueueFanoutRetry, evt)
}

func (p *Publisher) PublishToDeadLetter(ctx context.Context, evt domain.FanoutRetryEvent) error {
	return p.publishToExchange("", QueueFanoutDead, evt)
}

func (p *Publisher) publishToExchange(exchange, key string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch == nil {
		ch, err := p.openCh()
		if err != nil {
			return err
		}
		p.ch = ch
	}

	if err := p.ch.Publish(exchange, key, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}); err != nil {
		p.ch.Close()
		p.ch = nil
		return err
	}
	return nil
}

// DeadLetterPublisher adapta Publisher para implementar domain.FanoutRetryPublisher
// publicando a fanout.dead en lugar de fanout.retry.
type DeadLetterPublisher struct {
	pub *Publisher
}

func NewDeadLetterPublisher(pub *Publisher) *DeadLetterPublisher {
	return &DeadLetterPublisher{pub: pub}
}

func (d *DeadLetterPublisher) PublishFanoutRetry(ctx context.Context, evt domain.FanoutRetryEvent) error {
	return d.pub.PublishToDeadLetter(ctx, evt)
}
