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

func TestIntegration_TweetCreatedConsumer_FansOutToActiveFollowers(t *testing.T) {
	if testDB == nil {
		t.Skip("integration only")
	}
	truncate(t)
	flushRedis(t)

	// Arrange
	aliceID := uuid.New()
	bobID := uuid.New()
	seedUser(t, aliceID, "alice")
	seedUser(t, bobID, "bob")
	setLastActive(t, bobID, time.Now())
	seedFollow(t, bobID, aliceID)

	activityTTL := 24 * time.Hour
	followRepo := postgresrepo.NewFollowRepository(testDB)
	pgTimeline := postgresrepo.NewTimelineRepository(testDB)
	redisTimeline := redisrepo.NewTimelineRepository(testRDB, pgTimeline, 500).WithActivityTTL(activityTTL)
	fanoutUC := usecase.NewFanoutTweetUseCase(followRepo, redisTimeline, activityTTL)
	consumer := &TweetCreatedConsumer{svc: fanoutUC}

	tweetID := uuid.New()
	evt := domain.TweetCreatedEvent{
		TweetID:   tweetID,
		UserID:    aliceID,
		Username:  "alice",
		Content:   "hello world",
		CreatedAt: time.Now(),
	}
	body, _ := json.Marshal(evt)

	// Act
	acked, nacked := consumer.handleDelivery(context.Background(), amqp.Delivery{Body: body})

	// Assert: ack
	if !acked || nacked {
		t.Fatalf("want ack=true nack=false, got ack=%v nack=%v", acked, nacked)
	}

	// Assert: tweet in Bob's Redis timeline
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
