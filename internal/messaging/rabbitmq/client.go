package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	QueueTweetCreated  = "tweet.created"
	QueueFollowCreated = "follow.created"

	ExchangeFanoutRetry = "fanout.retry.exchange"
	QueueFanoutRetry    = "fanout.retry"

	ExchangeFanoutWait = "fanout.wait.exchange"
	QueueFanoutWait    = "fanout.wait"

	QueueFanoutDead = "fanout.dead"
)

// Connect dials RabbitMQ, declares the topology, and starts a background
// reconnect loop. The returned *ResilientConn is ready to use immediately.
func Connect(ctx context.Context, url string) (*ResilientConn, error) {
	dialFn := func(u string) (Conn, error) {
		conn, err := amqp.Dial(u)
		if err != nil {
			return nil, err
		}
		if err := declareTopology(conn); err != nil {
			conn.Close()
			return nil, err
		}
		return conn, nil
	}

	rc := newResilientConn(url, dialFn)
	go rc.reconnectLoop(ctx)

	select {
	case <-rc.ready:
		return rc, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func declareTopology(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	for _, q := range []string{QueueTweetCreated, QueueFollowCreated} {
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			return err
		}
	}

	if err := ch.ExchangeDeclare(ExchangeFanoutRetry, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(QueueFanoutRetry, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": ExchangeFanoutWait,
	}); err != nil {
		return err
	}
	if err := ch.QueueBind(QueueFanoutRetry, QueueFanoutRetry, ExchangeFanoutRetry, false, nil); err != nil {
		return err
	}

	if err := ch.ExchangeDeclare(ExchangeFanoutWait, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(QueueFanoutWait, true, false, false, false, amqp.Table{
		"x-message-ttl":          int32(30000),
		"x-dead-letter-exchange": ExchangeFanoutRetry,
	}); err != nil {
		return err
	}
	if err := ch.QueueBind(QueueFanoutWait, QueueFanoutWait, ExchangeFanoutWait, false, nil); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare(QueueFanoutDead, true, false, false, false, nil); err != nil {
		return err
	}

	return nil
}
