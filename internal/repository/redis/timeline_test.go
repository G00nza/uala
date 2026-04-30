package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"uala/internal/domain"
	redisrepo "uala/internal/repository/redis"
)

type mockPgTimeline struct {
	items []domain.TweetItem
	err   error
}

func (m *mockPgTimeline) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	return m.items, m.err
}

func TestRedisTimeline_AppendAndGet(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	authorID := uuid.New()

	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)

	item := domain.TweetItem{
		ID:        uuid.New(),
		UserID:    authorID,
		Username:  "bob",
		Content:   "hello from redis",
		CreatedAt: time.Now().UTC().Truncate(time.Second),
	}
	if err := repo.AppendTweet(context.Background(), userID, item); err != nil {
		t.Fatalf("AppendTweet: %v", err)
	}

	items, err := repo.GetTimeline(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
	if items[0].Content != "hello from redis" {
		t.Fatalf("want 'hello from redis', got %s", items[0].Content)
	}
	if items[0].Username != "bob" {
		t.Fatalf("want 'bob', got %s", items[0].Username)
	}
}

func TestRedisTimeline_MultipleItems_OrderedByScoreDesc(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)

	older := domain.TweetItem{
		ID: uuid.New(), UserID: uuid.New(), Username: "bob",
		Content: "older tweet", CreatedAt: time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Second),
	}
	newer := domain.TweetItem{
		ID: uuid.New(), UserID: uuid.New(), Username: "bob",
		Content: "newer tweet", CreatedAt: time.Now().UTC().Truncate(time.Second),
	}

	_ = repo.AppendTweet(context.Background(), userID, older)
	_ = repo.AppendTweet(context.Background(), userID, newer)

	items, err := repo.GetTimeline(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if items[0].Content != "newer tweet" {
		t.Fatalf("want newer first, got %s", items[0].Content)
	}
}

func TestRedisTimeline_FallbackToPostgresOnMiss(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	authorID := uuid.New()

	pgItems := []domain.TweetItem{
		{
			ID: uuid.New(), UserID: authorID, Username: "carol",
			Content: "from postgres", CreatedAt: time.Now().UTC().Truncate(time.Second),
		},
	}
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{items: pgItems}, 500)

	items, err := repo.GetTimeline(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item from postgres fallback, got %d", len(items))
	}
	if items[0].Content != "from postgres" {
		t.Fatalf("want 'from postgres', got %s", items[0].Content)
	}
}

func TestRedisTimeline_FallbackPopulatesRedis(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	authorID := uuid.New()

	pgItems := []domain.TweetItem{
		{
			ID: uuid.New(), UserID: authorID, Username: "dave",
			Content: "populate me", CreatedAt: time.Now().UTC().Truncate(time.Second),
		},
	}
	// First call with Postgres data
	repo1 := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{items: pgItems}, 500)
	_, _ = repo1.GetTimeline(context.Background(), userID)

	// Second call with empty Postgres — should read from Redis cache
	repo2 := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{items: []domain.TweetItem{}}, 500)
	items, err := repo2.GetTimeline(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetTimeline second call: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item from Redis cache on second call, got %d", len(items))
	}
	if items[0].Content != "populate me" {
		t.Fatalf("want 'populate me', got %s", items[0].Content)
	}
}

func TestRedisTimeline_EmptyTimeline_NoFallbackLoop(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()

	callCount := 0
	pg := &countingPgTimeline{items: []domain.TweetItem{}, countPtr: &callCount}
	repo := redisrepo.NewTimelineRepository(testRDB, pg, 500)

	// User with no followed tweets: Redis miss → Postgres → empty → no Redis write
	items, err := repo.GetTimeline(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("want 0 items, got %d", len(items))
	}
	if callCount != 1 {
		t.Fatalf("want 1 Postgres call, got %d", callCount)
	}
}

type countingPgTimeline struct {
	items    []domain.TweetItem
	countPtr *int
}

func (m *countingPgTimeline) GetTimeline(ctx context.Context, userID uuid.UUID) ([]domain.TweetItem, error) {
	*m.countPtr++
	return m.items, nil
}
