package rabbitmq

import (
	"errors"
	"sync/atomic"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// connFunc adapts a func to the channeler interface.
type connFunc func() (*amqp.Channel, error)

func (f connFunc) Channel() (*amqp.Channel, error) { return f() }

func TestPublisher_RetriesOnChannelOpenFailure(t *testing.T) {
	var attempts atomic.Int32
	p := &Publisher{conn: connFunc(func() (*amqp.Channel, error) {
		attempts.Add(1)
		return nil, errors.New("channel/connection is not open")
	})}

	err := p.publishToExchange("", "q", "payload")
	if err == nil {
		t.Fatal("expected error when all channel opens fail")
	}
	if attempts.Load() != 3 {
		t.Errorf("want 3 attempts, got %d", attempts.Load())
	}
}
