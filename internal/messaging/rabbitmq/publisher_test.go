package rabbitmq

import (
	"errors"
	"sync/atomic"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeAmqpChannel struct {
	publishErr error
	publishes  atomic.Int32
	closes     atomic.Int32
}

func (f *fakeAmqpChannel) Publish(_, _ string, _, _ bool, _ amqp.Publishing) error {
	f.publishes.Add(1)
	return f.publishErr
}

func (f *fakeAmqpChannel) Close() error {
	f.closes.Add(1)
	return nil
}

func TestPublisher_ReusesChannelAcrossPublishes(t *testing.T) {
	var opens atomic.Int32
	fc := &fakeAmqpChannel{}

	p := &Publisher{
		openCh: func() (amqpChannel, error) {
			opens.Add(1)
			return fc, nil
		},
	}

	for i := range 5 {
		if err := p.publishToExchange("", "q", "payload"); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	if opens.Load() != 1 {
		t.Errorf("want channel opened once for 5 publishes, got %d opens", opens.Load())
	}
	if fc.publishes.Load() != 5 {
		t.Errorf("want 5 Publish calls, got %d", fc.publishes.Load())
	}
}

func TestPublisher_ReopensChannelAfterPublishError(t *testing.T) {
	var opens atomic.Int32

	p := &Publisher{
		openCh: func() (amqpChannel, error) {
			fc := &fakeAmqpChannel{}
			if opens.Add(1) == 1 {
				fc.publishErr = errors.New("channel closed")
			}
			return fc, nil
		},
	}

	_ = p.publishToExchange("", "q", "payload") // fails, channel invalidated

	if err := p.publishToExchange("", "q", "payload"); err != nil {
		t.Fatalf("second publish after error: %v", err)
	}

	if opens.Load() != 2 {
		t.Errorf("want 2 channel opens after error, got %d", opens.Load())
	}
}
