package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"uala/internal/domain"
)

// stubFollowRepo returns a fixed list of followers.
type stubFollowRepo struct {
	followers []uuid.UUID
}

func (s *stubFollowRepo) Create(_ context.Context, _ *domain.Follow) error { return nil }
func (s *stubFollowRepo) Exists(_ context.Context, _, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (s *stubFollowRepo) GetFollowers(_ context.Context, _ uuid.UUID) ([]uuid.UUID, error) {
	return s.followers, nil
}

func (s *stubFollowRepo) GetActiveFollowers(_ context.Context, _ uuid.UUID, _ time.Time) ([]domain.FollowerActivity, error) {
	result := make([]domain.FollowerActivity, len(s.followers))
	for i, id := range s.followers {
		result[i] = domain.FollowerActivity{ID: id, LastActive: time.Now()}
	}
	return result, nil
}

// concurrentFanout records max concurrent calls to AppendTweet.
type concurrentFanout struct {
	mu          sync.Mutex
	active      int64
	maxObserved int64
}

func (f *concurrentFanout) AppendTweet(_ context.Context, _ uuid.UUID, _ domain.TweetItem, _ time.Duration) error {
	cur := atomic.AddInt64(&f.active, 1)
	f.mu.Lock()
	if cur > f.maxObserved {
		f.maxObserved = cur
	}
	f.mu.Unlock()
	time.Sleep(5 * time.Millisecond) // simulate Redis latency
	atomic.AddInt64(&f.active, -1)
	return nil
}

func TestHandleTweetCreated_FanoutIsConcurrent(t *testing.T) {
	const numFollowers = 50

	followers := make([]uuid.UUID, numFollowers)
	for i := range followers {
		followers[i] = uuid.New()
	}

	fanout := &concurrentFanout{}
	c := &Consumer{
		followRepo:    &stubFollowRepo{followers: followers},
		fanout:        fanout,
		fanoutWorkers: fanoutConcurrency,
	}

	evt := domain.TweetCreatedEvent{
		TweetID:   uuid.New(),
		UserID:    uuid.New(),
		Username:  "testuser",
		Content:   "hello",
		CreatedAt: time.Now(),
	}

	c.fanoutTweet(context.Background(), evt, followers)

	if fanout.maxObserved <= 1 {
		t.Errorf("expected concurrent fanout (max observed > 1), got %d", fanout.maxObserved)
	}
}

// errorFanout always returns an error for AppendTweet.
type errorFanout struct{}

func (e *errorFanout) AppendTweet(_ context.Context, _ uuid.UUID, _ domain.TweetItem, _ time.Duration) error {
	return errors.New("redis unavailable")
}

// partialErrorFanout fails for the first N calls, succeeds for the rest.
type partialErrorFanout struct {
	failCount int
	calls     atomic.Int64
}

func (p *partialErrorFanout) AppendTweet(_ context.Context, _ uuid.UUID, _ domain.TweetItem, _ time.Duration) error {
	if int(p.calls.Add(1)) <= p.failCount {
		return errors.New("redis unavailable")
	}
	return nil
}

func TestFanoutTweet_AllFail_ReturnsError(t *testing.T) {
	followers := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	c := &Consumer{fanout: &errorFanout{}, fanoutWorkers: fanoutConcurrency}

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()}
	err := c.fanoutTweet(context.Background(), evt, followers)

	if err == nil {
		t.Error("expected error when all AppendTweet calls fail, got nil")
	}
}

// recordingRetryPublisher captura los eventos publicados.
type recordingRetryPublisher struct {
	mu     sync.Mutex
	events []domain.FanoutRetryEvent
}

func (r *recordingRetryPublisher) PublishFanoutRetry(_ context.Context, evt domain.FanoutRetryEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
	return nil
}

func TestFanoutTweet_PublishesRetryOnAppendFailure(t *testing.T) {
	followerID := uuid.New()
	followers := []uuid.UUID{followerID}

	retryPub := &recordingRetryPublisher{}
	c := &Consumer{
		fanout:         &errorFanout{},
		retryPublisher: retryPub,
		fanoutWorkers:  fanoutConcurrency,
	}

	evt := domain.TweetCreatedEvent{
		TweetID:   uuid.New(),
		UserID:    uuid.New(),
		Content:   "hello",
		CreatedAt: time.Now(),
	}
	_ = c.fanoutTweet(context.Background(), evt, followers)

	retryPub.mu.Lock()
	defer retryPub.mu.Unlock()
	if len(retryPub.events) != 1 {
		t.Fatalf("want 1 retry event published, got %d", len(retryPub.events))
	}
	if retryPub.events[0].FollowerID != followerID {
		t.Errorf("want followerID %s, got %s", followerID, retryPub.events[0].FollowerID)
	}
	if retryPub.events[0].Tweet.Content != "hello" {
		t.Errorf("want tweet content 'hello', got %s", retryPub.events[0].Tweet.Content)
	}
}

func TestFanoutTweet_CountsRetryPublishAsHandled(t *testing.T) {
	followers := []uuid.UUID{uuid.New(), uuid.New()}

	c := &Consumer{
		fanout:         &errorFanout{},
		retryPublisher: &recordingRetryPublisher{},
		fanoutWorkers:  fanoutConcurrency,
	}

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()}
	err := c.fanoutTweet(context.Background(), evt, followers)

	if err != nil {
		t.Errorf("expected nil when retries are published, got %v", err)
	}
}

func TestFanoutTweet_PartialFail_ReturnsNil(t *testing.T) {
	followers := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	// first 2 fail, last 1 succeeds
	c := &Consumer{fanout: &partialErrorFanout{failCount: 2}, fanoutWorkers: fanoutConcurrency}

	evt := domain.TweetCreatedEvent{TweetID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()}
	err := c.fanoutTweet(context.Background(), evt, followers)

	if err != nil {
		t.Errorf("expected nil when at least one AppendTweet succeeds, got %v", err)
	}
}

func makeDelivery(body []byte, xDeathCount int64) amqp.Delivery {
	headers := amqp.Table{}
	if xDeathCount > 0 {
		headers["x-death"] = []interface{}{
			amqp.Table{
				"queue": QueueFanoutRetry,
				"count": xDeathCount,
			},
		}
	}
	return amqp.Delivery{Body: body, Headers: headers}
}

func TestHandleFanoutRetry_AcksOnSuccess(t *testing.T) {
	c := &Consumer{fanout: &concurrentFanout{}, fanoutWorkers: fanoutConcurrency}

	evt := domain.FanoutRetryEvent{
		FollowerID: uuid.New(),
		Tweet:      domain.TweetItem{ID: uuid.New(), Content: "hi", CreatedAt: time.Now()},
	}
	body, _ := json.Marshal(evt)
	d := makeDelivery(body, 0)

	acked, nacked := c.handleFanoutRetry(context.Background(), d)

	if !acked {
		t.Error("expected Ack on successful AppendTweet")
	}
	if nacked {
		t.Error("expected no Nack on success")
	}
}

func TestHandleFanoutRetry_NacksOnFailure(t *testing.T) {
	c := &Consumer{fanout: &errorFanout{}, fanoutWorkers: fanoutConcurrency}

	evt := domain.FanoutRetryEvent{
		FollowerID: uuid.New(),
		Tweet:      domain.TweetItem{ID: uuid.New(), CreatedAt: time.Now()},
	}
	body, _ := json.Marshal(evt)
	d := makeDelivery(body, 2)

	acked, nacked := c.handleFanoutRetry(context.Background(), d)

	if acked {
		t.Error("expected no Ack on failed AppendTweet")
	}
	if !nacked {
		t.Error("expected Nack on failed AppendTweet")
	}
}

func TestHandleFanoutRetry_DeadLettersAt10(t *testing.T) {
	deadPub := &recordingRetryPublisher{}
	c := &Consumer{
		fanout:        &errorFanout{},
		deadLetterPub: deadPub,
		fanoutWorkers: fanoutConcurrency,
	}

	followerID := uuid.New()
	evt := domain.FanoutRetryEvent{
		FollowerID: followerID,
		Tweet:      domain.TweetItem{ID: uuid.New(), CreatedAt: time.Now()},
	}
	body, _ := json.Marshal(evt)
	d := makeDelivery(body, 10)

	acked, nacked := c.handleFanoutRetry(context.Background(), d)

	if !acked {
		t.Error("expected Ack after dead-lettering")
	}
	if nacked {
		t.Error("expected no Nack after dead-lettering")
	}
	deadPub.mu.Lock()
	defer deadPub.mu.Unlock()
	if len(deadPub.events) != 1 {
		t.Fatalf("want 1 dead letter event, got %d", len(deadPub.events))
	}
	if deadPub.events[0].FollowerID != followerID {
		t.Errorf("dead letter event has wrong followerID")
	}
}
