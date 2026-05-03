package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

type stubFanoutTweetSvc struct {
	mu    sync.Mutex
	calls []domain.TweetCreatedEvent
	err   error
}

func (s *stubFanoutTweetSvc) Execute(_ context.Context, evt domain.TweetCreatedEvent) error {
	s.mu.Lock()
	s.calls = append(s.calls, evt)
	s.mu.Unlock()
	return s.err
}

func TestTweetCreatedConsumer_AcksOnSuccess(t *testing.T) {
	svc := &stubFanoutTweetSvc{}
	c := &TweetCreatedConsumer{svc: svc}

	evt := domain.TweetCreatedEvent{
		TweetID:   uuid.New(),
		UserID:    uuid.New(),
		Username:  "alice",
		Content:   "hello",
		CreatedAt: time.Now(),
	}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: body})

	if !acked {
		t.Error("expected ack on success")
	}
	if nacked {
		t.Error("expected no nack on success")
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.calls) != 1 {
		t.Fatalf("want 1 svc call, got %d", len(svc.calls))
	}
}

func TestTweetCreatedConsumer_NacksOnSvcError(t *testing.T) {
	svc := &stubFanoutTweetSvc{err: errors.New("fanout failed")}
	c := &TweetCreatedConsumer{svc: svc}

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New()}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: body})

	if acked {
		t.Error("expected no ack on svc error")
	}
	if !nacked {
		t.Error("expected nack on svc error")
	}
}

func TestTweetCreatedConsumer_NacksOnBadJSON(t *testing.T) {
	c := &TweetCreatedConsumer{svc: &stubFanoutTweetSvc{}}

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: []byte("bad-json")})

	if acked {
		t.Error("expected no ack on bad JSON")
	}
	if !nacked {
		t.Error("expected nack on bad JSON")
	}
}
