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

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func makeFanoutRetryDelivery(body []byte, deathCount int64) amqp.Delivery {
	headers := amqp.Table{}
	if deathCount > 0 {
		headers["x-death"] = []interface{}{
			amqp.Table{"queue": QueueFanoutRetry, "count": deathCount},
		}
	}
	return amqp.Delivery{Body: body, Headers: headers}
}

func TestIntegration_FanoutRetryConsumer_AppendsToRedisTimeline(t *testing.T) {
	if testDB == nil {
		t.Skip("integration only")
	}
	flushRedis(t)

	// Arrange
	followerID := uuid.New()
	tweetID := uuid.New()
	activityTTL := 24 * time.Hour
	pgTimeline := postgresrepo.NewTimelineRepository(testDB)
	redisTimeline := redisrepo.NewTimelineRepository(testRDB, pgTimeline, 500).WithActivityTTL(activityTTL)
	consumer := &FanoutRetryConsumer{fanout: redisTimeline, activityTTL: activityTTL}

	evt := domain.FanoutRetryEvent{
		FollowerID: followerID,
		Tweet: domain.TweetItem{
			ID:        tweetID,
			UserID:    uuid.New(),
			Username:  "alice",
			Content:   "retry me",
			CreatedAt: time.Now(),
		},
	}
	body, _ := json.Marshal(evt)

	// Act
	acked, nacked := consumer.handleDelivery(context.Background(), makeFanoutRetryDelivery(body, 0))

	// Assert: ack
	if !acked || nacked {
		t.Fatalf("want ack=true nack=false, got ack=%v nack=%v", acked, nacked)
	}

	// Assert: tweet written to Redis
	count, err := testRDB.ZCard(context.Background(), "timeline:"+followerID.String()).Result()
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if count != 1 {
		t.Errorf("want 1 tweet in redis timeline, got %d", count)
	}
}

type stubTweetAppender struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (s *stubTweetAppender) AppendTweet(_ context.Context, _ uuid.UUID, _ domain.TweetItem, _ time.Duration) error {
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

func TestFanoutRetryConsumer_AcksOnSuccess(t *testing.T) {
	appender := &stubTweetAppender{}
	c := &FanoutRetryConsumer{fanout: appender, activityTTL: 24 * time.Hour}

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
	c := &FanoutRetryConsumer{fanout: appender, activityTTL: 24 * time.Hour}

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
		fanout:        &stubTweetAppender{err: errors.New("redis down")},
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
	c := &FanoutRetryConsumer{fanout: &stubTweetAppender{}, activityTTL: 24 * time.Hour}

	acked, nacked := c.handleDelivery(context.Background(), amqp.Delivery{Body: []byte("bad-json")})

	if acked {
		t.Error("expected no ack on bad JSON")
	}
	if !nacked {
		t.Error("expected nack on bad JSON")
	}
}
