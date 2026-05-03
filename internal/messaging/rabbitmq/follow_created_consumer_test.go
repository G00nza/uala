package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"uala/internal/domain"
	postgresrepo "uala/internal/repository/postgres"
	redisrepo "uala/internal/repository/redis"
	"uala/internal/usecase"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestIntegration_FollowCreatedConsumer_BackfillsTweetsToFollower(t *testing.T) {
	if testDB == nil {
		t.Skip("integration only")
	}
	truncate(t)
	flushRedis(t)

	// Arrange
	aliceID := uuid.New()
	bobID := uuid.New()
	tweetID := uuid.New()
	seedUser(t, aliceID, "alice")
	seedUser(t, bobID, "bob")
	seedTweet(t, tweetID, aliceID, "hello from alice", time.Now())

	activityTTL := 24 * time.Hour
	pgTimeline := postgresrepo.NewTimelineRepository(testDB)
	redisTimeline := redisrepo.NewTimelineRepository(testRDB, pgTimeline, 500).WithActivityTTL(activityTTL)
	backfillUC := usecase.NewBackfillTimelineUseCase(pgTimeline, redisTimeline, 20, activityTTL)
	consumer := &FollowCreatedConsumer{svc: backfillUC}

	evt := domain.FollowCreatedEvent{FollowerID: bobID, FolloweeID: aliceID}
	body, _ := json.Marshal(evt)

	// Act
	acked, nacked := consumer.handleDelivery(context.Background(), amqp.Delivery{Body: body})

	// Assert: ack
	if !acked || nacked {
		t.Fatalf("want ack=true nack=false, got ack=%v nack=%v", acked, nacked)
	}

	// Assert: alice's tweet in Bob's Redis timeline
	items, err := redisTimeline.GetTimeline(context.Background(), domain.TimelineQuery{UserID: bobID, Limit: 10})
	if err != nil {
		t.Fatalf("get bob timeline: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 tweet in bob's timeline, got %d", len(items))
	}
	if items[0].ID != tweetID {
		t.Errorf("want tweet %s in bob's timeline, got %s", tweetID, items[0].ID)
	}
}

type stubBackfillSvc struct {
	mu    sync.Mutex
	calls []domain.FollowCreatedEvent
	err   error
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
