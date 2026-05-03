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

type stubTweetAppender struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (s *stubTweetAppender) Execute(_ context.Context, _ uuid.UUID, _ domain.TweetItem, _ time.Duration) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.err
}

type stubDeadLetterPub struct {
	mu     sync.Mutex
	events []domain.FanoutRetryEvent
	err    error
}

func (s *stubDeadLetterPub) PublishFanoutRetry(_ context.Context, evt domain.FanoutRetryEvent) error {
	s.mu.Lock()
	s.events = append(s.events, evt)
	s.mu.Unlock()
	return s.err
}

func makeFanoutRetryDelivery(body []byte, deathCount int64) amqp.Delivery {
	headers := amqp.Table{}
	if deathCount > 0 {
		headers["x-death"] = []interface{}{
			amqp.Table{"queue": QueueFanoutRetry, "count": deathCount},
		}
	}
	return amqp.Delivery{Body: body, Headers: headers}
}

func TestFanoutRetryConsumer_AcksOnSuccess(t *testing.T) {
	appender := &stubTweetAppender{}
	c := &FanoutRetryConsumer{svc: appender, activityTTL: 24 * time.Hour}

	evt := domain.FanoutRetryEvent{FollowerID: uuid.New(), Tweet: domain.TweetItem{ID: uuid.New(), CreatedAt: time.Now()}}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), makeFanoutRetryDelivery(body, 0))

	if !acked {
		t.Error("expected ack on success")
	}
	if nacked {
		t.Error("expected no nack on success")
	}
}

func TestFanoutRetryConsumer_NacksOnAppendFailure(t *testing.T) {
	appender := &stubTweetAppender{err: errors.New("redis down")}
	c := &FanoutRetryConsumer{svc: appender, activityTTL: 24 * time.Hour}

	evt := domain.FanoutRetryEvent{FollowerID: uuid.New(), Tweet: domain.TweetItem{ID: uuid.New(), CreatedAt: time.Now()}}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), makeFanoutRetryDelivery(body, 2))

	if acked {
		t.Error("expected no ack on failure")
	}
	if !nacked {
		t.Error("expected nack on failure")
	}
}

func TestFanoutRetryConsumer_DeadLettersAt10(t *testing.T) {
	dlq := &stubDeadLetterPub{}
	followerID := uuid.New()
	c := &FanoutRetryConsumer{
		svc:           &stubTweetAppender{err: errors.New("redis down")},
		deadLetterPub: dlq,
		activityTTL:   24 * time.Hour,
	}

	evt := domain.FanoutRetryEvent{FollowerID: followerID, Tweet: domain.TweetItem{ID: uuid.New(), CreatedAt: time.Now()}}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), makeFanoutRetryDelivery(body, 10))

	if !acked {
		t.Error("expected ack after dead-lettering")
	}
	if nacked {
		t.Error("expected no nack after dead-lettering")
	}
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	if len(dlq.events) != 1 {
		t.Fatalf("want 1 dead-letter event, got %d", len(dlq.events))
	}
	if dlq.events[0].FollowerID != followerID {
		t.Errorf("wrong followerID in dead-letter event")
	}
}

func TestFanoutRetryConsumer_NacksOnBadJSON(t *testing.T) {
	c := &FanoutRetryConsumer{svc: &stubTweetAppender{}, activityTTL: 24 * time.Hour}

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: []byte("bad-json")})

	if acked {
		t.Error("expected no ack on bad JSON")
	}
	if !nacked {
		t.Error("expected nack on bad JSON")
	}
}
