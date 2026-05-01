package rabbitmq

import (
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

func Connect(url string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	defer ch.Close()

	// colas pre-existentes
	for _, q := range []string{QueueTweetCreated, QueueFollowCreated} {
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			conn.Close()
			return nil, err
		}
	}

	// fanout.retry.exchange → fanout.retry (con DLX a fanout.wait.exchange)
	if err := ch.ExchangeDeclare(ExchangeFanoutRetry, "direct", true, false, false, false, nil); err != nil {
		conn.Close()
		return nil, err
	}
	if _, err := ch.QueueDeclare(QueueFanoutRetry, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": ExchangeFanoutWait,
	}); err != nil {
		conn.Close()
		return nil, err
	}
	if err := ch.QueueBind(QueueFanoutRetry, QueueFanoutRetry, ExchangeFanoutRetry, false, nil); err != nil {
		conn.Close()
		return nil, err
	}

	// fanout.wait.exchange → fanout.wait (TTL 30s, DLX de vuelta a fanout.retry.exchange)
	if err := ch.ExchangeDeclare(ExchangeFanoutWait, "direct", true, false, false, false, nil); err != nil {
		conn.Close()
		return nil, err
	}
	if _, err := ch.QueueDeclare(QueueFanoutWait, true, false, false, false, amqp.Table{
		"x-message-ttl":          int32(30000),
		"x-dead-letter-exchange": ExchangeFanoutRetry,
	}); err != nil {
		conn.Close()
		return nil, err
	}
	if err := ch.QueueBind(QueueFanoutWait, QueueFanoutWait, ExchangeFanoutWait, false, nil); err != nil {
		conn.Close()
		return nil, err
	}

	// fanout.dead — cola final para replay manual
	if _, err := ch.QueueDeclare(QueueFanoutDead, true, false, false, false, nil); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}
