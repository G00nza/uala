package redis_test

import (
	"context"
	"fmt"
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

func (m *mockPgTimeline) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
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
	if err := repo.AppendTweet(context.Background(), userID, item, 0); err != nil {
		t.Fatalf("AppendTweet: %v", err)
	}

	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Limit: 500})
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

	_ = repo.AppendTweet(context.Background(), userID, older, 0)
	_ = repo.AppendTweet(context.Background(), userID, newer, 0)

	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Limit: 500})
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

	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Limit: 500})
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
	_, _ = repo1.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Limit: 500})

	// Second call with empty Postgres — should read from Redis cache
	repo2 := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{items: []domain.TweetItem{}}, 500)
	items, err := repo2.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Limit: 500})
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
	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Limit: 500})
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

func TestRedisTimeline_AppendTweet_DeduplicatesByTweetID(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)

	tweetID := uuid.New()
	base := domain.TweetItem{
		ID:        tweetID,
		UserID:    uuid.New(),
		Username:  "alice",
		Content:   "original",
		CreatedAt: time.Now().UTC().Truncate(time.Second),
	}
	// Same tweet ID, different content — simulates redelivered message with any byte difference.
	redelivered := base
	redelivered.Content = "redelivered"

	_ = repo.AppendTweet(context.Background(), userID, base, 0)
	_ = repo.AppendTweet(context.Background(), userID, redelivered, 0)

	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Limit: 500})
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item for same tweet ID, got %d", len(items))
	}
}

func TestRedisTimeline_AppendTweet_SetsExpireNX(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)

	item := domain.TweetItem{
		ID: uuid.New(), UserID: uuid.New(), Username: "alice",
		Content: "ttl test", CreatedAt: time.Now().UTC().Truncate(time.Second),
	}
	ttl := 10 * time.Hour
	_ = repo.AppendTweet(context.Background(), userID, item, ttl)

	key := "timeline:" + userID.String()
	remaining, err := testRDB.TTL(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if remaining <= 0 {
		t.Errorf("want positive TTL after AppendTweet, got %v", remaining)
	}
	if remaining > ttl {
		t.Errorf("TTL %v exceeds requested %v", remaining, ttl)
	}
}

func TestRedisTimeline_AppendTweet_ExpireNX_DoesNotShortenExistingTTL(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)

	item1 := domain.TweetItem{
		ID: uuid.New(), UserID: uuid.New(), Username: "alice",
		Content: "first", CreatedAt: time.Now().UTC().Truncate(time.Second),
	}
	item2 := domain.TweetItem{
		ID: uuid.New(), UserID: uuid.New(), Username: "alice",
		Content: "second", CreatedAt: time.Now().UTC().Truncate(time.Second),
	}

	longTTL := 20 * time.Hour
	shortTTL := 1 * time.Hour

	_ = repo.AppendTweet(context.Background(), userID, item1, longTTL)
	_ = repo.AppendTweet(context.Background(), userID, item2, shortTTL)

	key := "timeline:" + userID.String()
	remaining, _ := testRDB.TTL(context.Background(), key).Result()
	// ExpireNX must not overwrite an existing TTL — remaining must still be close to longTTL
	if remaining < 19*time.Hour {
		t.Errorf("ExpireNX should not shorten existing TTL: got %v, want > 19h", remaining)
	}
}

func TestRedisTimeline_GetTimeline_RefreshesTTL(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500).
		WithActivityTTL(24 * time.Hour)

	item := domain.TweetItem{
		ID: uuid.New(), UserID: uuid.New(), Username: "alice",
		Content: "refresh ttl", CreatedAt: time.Now().UTC().Truncate(time.Second),
	}
	// Write with a short TTL (simulating near-expired follower)
	_ = repo.AppendTweet(context.Background(), userID, item, 1*time.Hour)

	// Reading should refresh TTL to the full activityTTL
	_, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Limit: 500})
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}

	key := "timeline:" + userID.String()
	remaining, _ := testRDB.TTL(context.Background(), key).Result()
	if remaining < 23*time.Hour {
		t.Errorf("GetTimeline should refresh TTL to activityTTL: got %v, want > 23h", remaining)
	}
}

type countingPgTimeline struct {
	items    []domain.TweetItem
	countPtr *int
}

func (m *countingPgTimeline) GetTimeline(ctx context.Context, q domain.TimelineQuery) ([]domain.TweetItem, error) {
	*m.countPtr++
	return m.items, nil
}

func TestRedisTimeline_AfterCursor_InRedis(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)

	base := time.Now().UTC()
	t1 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet1", CreatedAt: base.Add(-2 * time.Minute).Truncate(time.Second)}
	t2 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet2", CreatedAt: base.Add(-1 * time.Minute).Truncate(time.Second)}
	t3 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet3", CreatedAt: base.Truncate(time.Second)}

	_ = repo.AppendTweet(context.Background(), userID, t1, 0)
	_ = repo.AppendTweet(context.Background(), userID, t2, 0)
	_ = repo.AppendTweet(context.Background(), userID, t3, 0)

	// after=t3 → older than t3 → [t2, t1]
	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, After: &t3.ID, Limit: 20})
	if err != nil {
		t.Fatalf("GetTimeline after: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if items[0].Content != "tweet2" {
		t.Fatalf("want tweet2 first, got %s", items[0].Content)
	}
	if items[1].Content != "tweet1" {
		t.Fatalf("want tweet1 second, got %s", items[1].Content)
	}
}

func TestRedisTimeline_BeforeCursor_InRedis(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)

	base := time.Now().UTC()
	t1 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet1", CreatedAt: base.Add(-2 * time.Minute).Truncate(time.Second)}
	t2 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet2", CreatedAt: base.Add(-1 * time.Minute).Truncate(time.Second)}
	t3 := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "tweet3", CreatedAt: base.Truncate(time.Second)}

	_ = repo.AppendTweet(context.Background(), userID, t1, 0)
	_ = repo.AppendTweet(context.Background(), userID, t2, 0)
	_ = repo.AppendTweet(context.Background(), userID, t3, 0)

	// before=t1 → newer than t1 → [t3, t2] DESC
	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Before: &t1.ID, Limit: 20})
	if err != nil {
		t.Fatalf("GetTimeline before: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if items[0].Content != "tweet3" {
		t.Fatalf("want tweet3 first (newest), got %s", items[0].Content)
	}
	if items[1].Content != "tweet2" {
		t.Fatalf("want tweet2 second, got %s", items[1].Content)
	}
}

func TestRedisTimeline_CursorNotInRedis_FallsBackToPostgres(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	ghostID := uuid.New()

	pgItems := []domain.TweetItem{
		{ID: uuid.New(), UserID: uuid.New(), Username: "pg", Content: "from_pg", CreatedAt: time.Now().UTC().Truncate(time.Second)},
	}
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{items: pgItems}, 500)

	// Seed one tweet so the Redis key exists
	seed := domain.TweetItem{ID: uuid.New(), UserID: uuid.New(), Username: "bob", Content: "in_redis", CreatedAt: time.Now().UTC().Truncate(time.Second)}
	_ = repo.AppendTweet(context.Background(), userID, seed, 0)

	// Use a cursor NOT in Redis → fallback to Postgres
	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, After: &ghostID, Limit: 20})
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item from postgres, got %d", len(items))
	}
	if items[0].Content != "from_pg" {
		t.Fatalf("want 'from_pg', got %s", items[0].Content)
	}
}

func TestRedisTimeline_FirstPage_ReturnsLimit(t *testing.T) {
	flushRedis(t)
	userID := uuid.New()
	repo := redisrepo.NewTimelineRepository(testRDB, &mockPgTimeline{}, 500)

	base := time.Now().UTC()
	for i := 0; i < 5; i++ {
		item := domain.TweetItem{
			ID:        uuid.New(),
			UserID:    uuid.New(),
			Username:  "bob",
			Content:   fmt.Sprintf("tweet%d", i),
			CreatedAt: base.Add(time.Duration(i) * time.Second).Truncate(time.Second),
		}
		_ = repo.AppendTweet(context.Background(), userID, item, 0)
	}

	items, err := repo.GetTimeline(context.Background(), domain.TimelineQuery{UserID: userID, Limit: 3})
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("want 3 items, got %d", len(items))
	}
}
