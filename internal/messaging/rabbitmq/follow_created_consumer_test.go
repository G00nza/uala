package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

type stubBackfillSvc struct {
	mu         sync.Mutex
	calls      []domain.FollowCreatedEvent
	err        error
}

func (s *stubBackfillSvc) Execute(_ context.Context, followerID, followeeID uuid.UUID) error {
	s.mu.Lock()
	s.calls = append(s.calls, domain.FollowCreatedEvent{FollowerID: followerID, FolloweeID: followeeID})
	s.mu.Unlock()
	return s.err
}

func TestFollowCreatedConsumer_AcksOnSuccess(t *testing.T) {
	svc := &stubBackfillSvc{}
	c := &FollowCreatedConsumer{svc: svc}

	evt := domain.FollowCreatedEvent{FollowerID: uuid.New(), FolloweeID: uuid.New()}
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

func TestFollowCreatedConsumer_NacksOnSvcError(t *testing.T) {
	svc := &stubBackfillSvc{err: errors.New("backfill failed")}
	c := &FollowCreatedConsumer{svc: svc}

	evt := domain.FollowCreatedEvent{FollowerID: uuid.New(), FolloweeID: uuid.New()}
	body, _ := json.Marshal(evt)

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: body})

	if acked {
		t.Error("expected no ack on svc error")
	}
	if !nacked {
		t.Error("expected nack on svc error")
	}
}

func TestFollowCreatedConsumer_NacksOnBadJSON(t *testing.T) {
	c := &FollowCreatedConsumer{svc: &stubBackfillSvc{}}

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: []byte("bad-json")})

	if acked {
		t.Error("expected no ack on bad JSON")
	}
	if !nacked {
		t.Error("expected nack on bad JSON")
	}
}
