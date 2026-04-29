package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	QueueTweetCreated  = "tweet.created"
	QueueFollowCreated = "follow.created"
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

	for _, q := range []string{QueueTweetCreated, QueueFollowCreated} {
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			conn.Close()
			return nil, err
		}
	}

	return conn, nil
}
